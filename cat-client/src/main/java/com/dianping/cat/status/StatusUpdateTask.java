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
package com.dianping.cat.status;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.helper.Threads.Task;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.dianping.cat.Cat;
import com.dianping.cat.configuration.ClientConfigManager;
import com.dianping.cat.configuration.NetworkInterfaceManager;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Heartbeat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.MessageProducer;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.internal.MilliSecondTimer;
import com.dianping.cat.message.spi.MessageStatistics;
import com.dianping.cat.status.model.entity.Extension;
import com.dianping.cat.status.model.entity.StatusInfo;

@Named
public class StatusUpdateTask implements Task, Initializable {
	@Inject
	private MessageStatistics m_statistics;

	@Inject
	private ClientConfigManager m_manager;

	private boolean m_active = true;

	private String m_ipAddress;

	private long m_interval = 60 * 1000; // 60 seconds

	private String m_jars;

	private void buildClasspath() {
		ClassLoader loader = StatusUpdateTask.class.getClassLoader();
		StringBuilder sb = new StringBuilder();

		buildClasspath(loader, sb);
		if (sb.length() > 0) {
			m_jars = sb.substring(0, sb.length() - 1);
		}
	}

	private void buildClasspath(ClassLoader loader, StringBuilder sb) {
		if (loader instanceof URLClassLoader) {
			URL[] urLs = ((URLClassLoader) loader).getURLs();
			for (URL url : urLs) {
				String jar = parseJar(url.toExternalForm());

				if (jar != null) {
					sb.append(jar).append(',');
				}
			}
			ClassLoader parent = loader.getParent();

			buildClasspath(parent, sb);
		}
	}

	private void buildExtensionData(StatusInfo status) {
		StatusExtensionRegister res = StatusExtensionRegister.getInstance();
		List<StatusExtension> extensions = res.getStatusExtension();
		int length = extensions.size();

		for (int i = 0; i < length; i++) {
			StatusExtension extension = extensions.get(i);
			String id = extension.getId();
			String des = extension.getDescription();
			Map<String, String> propertis = extension.getProperties();
			Extension item = status.findOrCreateExtension(id).setDescription(des);

			for (Entry<String, String> entry : propertis.entrySet()) {
				try {
					double value = Double.parseDouble(entry.getValue());
					item.findOrCreateExtensionDetail(entry.getKey()).setValue(value);
				} catch (Exception e) {
					Cat.logError("StatusExtension can only be double type", e);
				}
			}
		}
	}

	@Override
	public String getName() {
		return "StatusUpdateTask";
	}

	@Override
	public void initialize() throws InitializationException {
		m_ipAddress = NetworkInterfaceManager.INSTANCE.getLocalHostAddress();
	}

	private String parseJar(String path) {
		if (path.endsWith(".jar")) {
			int index = path.lastIndexOf('/');

			if (index > -1) {
				return path.substring(index + 1);
			}
		}
		return null;
	}

	@Override
	public void run() {
		// try to wait cat client init success
		try {
			Thread.sleep(10 * 1000);
		} catch (InterruptedException e) {
			return;
		}

		while (true) {
			Calendar cal = Calendar.getInstance();
			int second = cal.get(Calendar.SECOND);

			// try to avoid send heartbeat at 59-01 second
			//尝试避开在59-01秒这个期间发送心跳检查，因为这个点是统计聚合的高峰期
			if (second < 2 || second > 58) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// ignore it
				}
			} else {
				break;
			}
		}

		try {
			buildClasspath();//获得jar包路径
		} catch (Exception e) {
			e.printStackTrace();
		}
		MessageProducer cat = Cat.getProducer();
		Transaction reboot = cat.newTransaction("System", "Reboot");

		reboot.setStatus(Message.SUCCESS);
		cat.logEvent("Reboot", NetworkInterfaceManager.INSTANCE.getLocalHostAddress(), Message.SUCCESS, null);
		reboot.complete();

		while (m_active) {
			long start = MilliSecondTimer.currentTimeMillis();

			if (m_manager.isCatEnabled()) {
				//开始创建心跳的事务，并发送
				Transaction t = cat.newTransaction("System", "Status");
				Heartbeat h = cat.newHeartbeat("Heartbeat", m_ipAddress);
				StatusInfo status = new StatusInfo();

				t.addData("dumpLocked", m_manager.isDumpLocked());
				//StatusInfoCollector通过大量访问者模式的代码实现了将各种指标set到status中的功能，
				// 之后将status封装到HeartBeatMessage中，按照一般对于message的处理流程，flush到消息传输层中
				StatusInfoCollector collector = new StatusInfoCollector(m_statistics, m_jars);

				try {
					//收集客户机系统信息，包括磁盘、内存、线程、jvm等，并发送给cat-core
					status.accept(collector.setDumpLocked(m_manager.isDumpLocked()));

					buildExtensionData(status);
					h.addData(status.toString());
					h.setStatus(Message.SUCCESS);
				} catch (Throwable e) {
					h.setStatus(e);
					cat.logError(e);
				} finally {
					h.complete();
				}
				Cat.logEvent("Heartbeat", "jstack", Event.SUCCESS, collector.getJstackInfo());
				t.setStatus(Message.SUCCESS);
				t.complete();
			}

			try {
				//整除，换算成分钟
				long current = System.currentTimeMillis() / 1000 / 60;
				int min = (int) (current % (60));//计算下当前是一个小时的第几分钟

				// refresh config 3 minute
				if (min % 3 == 0) {//如果当前所处分钟整除3，就刷新下客户端配置
					m_manager.refreshConfig();
				}
			} catch (Exception e) {
				// ignore
			}

			long elapsed = MilliSecondTimer.currentTimeMillis() - start;
			//每隔60s发送一次心跳，并获得一次客户端的内存和系统信息
			if (elapsed < m_interval) {
				try {
					Thread.sleep(m_interval - elapsed);
				} catch (InterruptedException e) {
					break;
				}
			}
		}
	}

	public void setInterval(long interval) {
		m_interval = interval;
	}

	@Override
	public void shutdown() {
		m_active = false;
	}
}
