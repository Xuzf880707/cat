/*
 * Copyright (c) 2011-2018, Meituan Dianping. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dianping.cat.message.io;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.unidal.helper.Threads;
import org.unidal.helper.Threads.Task;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.dianping.cat.ApplicationSettings;
import com.dianping.cat.analyzer.LocalAggregator;
import com.dianping.cat.configuration.ClientConfigManager;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.internal.DefaultTransaction;
import com.dianping.cat.message.internal.MessageIdFactory;
import com.dianping.cat.message.spi.MessageCodec;
import com.dianping.cat.message.spi.MessageQueue;
import com.dianping.cat.message.spi.MessageStatistics;
import com.dianping.cat.message.spi.MessageTree;
import com.dianping.cat.message.spi.codec.NativeMessageCodec;
import com.dianping.cat.message.spi.internal.DefaultMessageTree;
import com.dianping.cat.status.StatusExtension;
import com.dianping.cat.status.StatusExtensionRegister;

@Named
public class TcpSocketSender implements Task, MessageSender, LogEnabled {

	public static final int SIZE = ApplicationSettings.getQueueSize();

	private static final int MAX_CHILD_NUMBER = 200;

	private static final int MAX_DURATION = 1000 * 30;

	public static final long HOUR = 1000 * 60 * 60L;

	private MessageCodec m_codec = new NativeMessageCodec();

	@Inject
	private MessageStatistics m_statistics;

	@Inject
	private ClientConfigManager m_configManager;

	@Inject
	private MessageIdFactory m_factory;

	private MessageQueue m_queue = new DefaultMessageQueue(SIZE);

	private MessageQueue m_atomicQueue = new DefaultMessageQueue(SIZE);

	private ChannelManager m_channelManager;

	private Logger m_logger;

	private boolean m_active;

	private AtomicInteger m_errors = new AtomicInteger();

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

	@Override
	public String getName() {
		return "TcpSocketSender";
	}

	/***
	 * 初始化netty客户端
	 * @param addresses
	 */
	@Override
	public void initialize(List<InetSocketAddress> addresses) {
		m_channelManager = new ChannelManager(m_logger, addresses, m_configManager, m_factory);
		//启动当前客户端，开启一个上报线程，上报线程一直读取内存队列，获取要发送的消息树，
		// 调用 sendInternal(MessageTree tree) 方法将消息树发送到服务器。
		Threads.forGroup("cat").start(this);
		Threads.forGroup("cat").start(m_channelManager);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				m_logger.info("shut down cat client in runtime shut down hook!");
				shutdown();
			}
		});

		StatusExtensionRegister.getInstance().register(new StatusExtension() {

			@Override
			public String getDescription() {
				return "client-send-queue";
			}

			@Override
			public String getId() {
				return "client-send-queue";
			}

			@Override
			public Map<String, String> getProperties() {
				Map<String, String> map = new HashMap<String, String>();

				map.put("msg-queue", String.valueOf(m_queue.size()));
				map.put("atomic-queue", String.valueOf(m_queue.size()));
				return map;
			}
		});
	}

	private void logQueueFullInfo(MessageTree tree) {
		if (m_statistics != null) {
			m_statistics.onOverflowed(tree);
		}

		int count = m_errors.incrementAndGet();

		if (count % 1000 == 0 || count == 1) {
			m_logger.error("Message queue is full in tcp socket sender! Count: " + count);
		}

		tree = null;
	}

	private MessageTree mergeTree(MessageQueue handler) {
		int max = MAX_CHILD_NUMBER;
		DefaultTransaction tran = new DefaultTransaction("System", "_CatMergeTree", null);
		MessageTree first = handler.poll();

		tran.setStatus(Transaction.SUCCESS);
		tran.setCompleted(true);
		tran.setDurationInMicros(0);
		tran.addChild(first.getMessage());

		while (max >= 0) {
			MessageTree tree = handler.poll();

			if (tree == null) {
				break;
			}
			tran.addChild(tree.getMessage());
			max--;
		}
		((DefaultMessageTree) first).setMessage(tran);
		return first;
	}

	private void offer(MessageTree tree) {
		if (m_configManager.isAtomicMessage(tree)) {
			boolean result = m_atomicQueue.offer(tree);

			if (!result) {
				logQueueFullInfo(tree);
			}
		} else {
			boolean result = m_queue.offer(tree);

			if (!result) {
				logQueueFullInfo(tree);
			}
		}
	}

	private void processAtomicMessage() {
		while (true) {
			if (shouldMerge(m_atomicQueue)) {
				MessageTree tree = mergeTree(m_atomicQueue);
				boolean result = m_queue.offer(tree);

				if (!result) {
					logQueueFullInfo(tree);
				}
			} else {
				break;
			}
		}
	}

	private void processNormalMessage() {
		while (true) {
			ChannelFuture channel = m_channelManager.channel();

			if (channel != null) {
				try {
					MessageTree tree = m_queue.poll();

					if (tree != null) {
						sendInternal(channel, tree);
						tree.setMessage(null);
					} else {
						try {
							Thread.sleep(5);
						} catch (Exception e) {
							m_active = false;
						}
						break;
					}
				} catch (Throwable t) {
					m_logger.error("Error when sending message over TCP socket!", t);
				}
			} else {
				try {
					Thread.sleep(5);
				} catch (Exception e) {
					m_active = false;
				}
			}
		}
	}
	//上报线程一直读取内存队列，获取要发送的消息树，调用 sendInternal(MessageTree tree) 方法将消息树发送到服务器。
	//客户端就实现了消息的异步化、队列化，从而保证日志的记录不会因为CAT系统异常而影响主业务线程。
	@Override
	public void run() {
		m_active = true;

		while (m_active) {//判断服务端是否存活
			processAtomicMessage();
			processNormalMessage();
		}

		processAtomicMessage();

		while (true) {
			MessageTree tree = m_queue.poll();

			if (tree != null) {
				ChannelFuture channel = m_channelManager.channel();

				if (channel != null) {
					sendInternal(channel, tree);
				} else {
					offer(tree);
				}
			} else {
				break;
			}
		}
	}

	@Override
	public void send(MessageTree tree) {
		if (!m_configManager.isBlock()) {
			double sampleRatio = m_configManager.getSampleRatio();

			if (tree.canDiscard() && sampleRatio < 1.0 && (!tree.isHitSample())) {
				processTreeInClient(tree);
			} else {
				offer(tree);//插入内存队列
			}
		}
	}

	private void processTreeInClient(MessageTree tree) {
		LocalAggregator.aggregate(tree);
	}

	public void sendInternal(ChannelFuture channel, MessageTree tree) {
		if (tree.getMessageId() == null) {//判断是否分配MessageID，没有则分配
			tree.setMessageId(m_factory.getNextId());
		}
		//对消息树进行序列化
		//通过JDK自带的公共接口编码的字节会有很多冗余信息来保证不同对象与字节之间的正确编解码
		//通过自定义的序列化方案可以节省许多不必要的字节信息，保证网络传输的高效性。
		ByteBuf buf = m_codec.encode(tree);

		int size = buf.readableBytes();

		channel.channel().writeAndFlush(buf);

		if (m_statistics != null) {
			m_statistics.onBytes(size);
		}
	}

	private boolean shouldMerge(MessageQueue queue) {
		MessageTree tree = queue.peek();

		if (tree != null) {
			long firstTime = tree.getMessage().getTimestamp();

			if (System.currentTimeMillis() - firstTime > MAX_DURATION || queue.size() >= MAX_CHILD_NUMBER) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void shutdown() {
		m_active = false;
		m_channelManager.shutdown();
	}
}
