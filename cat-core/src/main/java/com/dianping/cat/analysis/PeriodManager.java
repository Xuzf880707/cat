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
package com.dianping.cat.analysis;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.plexus.logging.Logger;
import org.unidal.helper.Threads;
import org.unidal.helper.Threads.Task;
import org.unidal.lookup.annotation.Inject;

import com.dianping.cat.Cat;
import com.dianping.cat.statistic.ServerStatisticManager;

public class PeriodManager implements Task {
	public static long EXTRATIME = 3 * 60 * 1000L;

	private PeriodStrategy m_strategy;

	private List<Period> m_periods = new ArrayList<Period>();

	private boolean m_active;

	@Inject
	private MessageAnalyzerManager m_analyzerManager;

	@Inject
	private ServerStatisticManager m_serverStateManager;

	@Inject
	private Logger m_logger;

	public PeriodManager(long duration, MessageAnalyzerManager analyzerManager,	ServerStatisticManager serverStateManager,
							Logger logger) {
		m_strategy = new PeriodStrategy(duration, EXTRATIME, EXTRATIME);
		m_active = true;
		m_analyzerManager = analyzerManager;
		m_serverStateManager = serverStateManager;
		m_logger = logger;
	}

	private void endPeriod(long startTime) {
		int len = m_periods.size();

		for (int i = 0; i < len; i++) {
			Period period = m_periods.get(i);

			if (period.isIn(startTime)) {
				period.finish();
				m_periods.remove(i);
				break;
			}
		}
	}

	public Period findPeriod(long timestamp) {
		for (Period period : m_periods) {
			if (period.isIn(timestamp)) {
				return period;
			}
		}

		return null;
	}

	@Override
	public String getName() {
		return "RealtimeConsumer-PeriodManager";
	}
	//计算当前时间所处的周期的开始时间，是当前时间的整点时间，
	// 比如现在是 13:50， 那么startTime=13:00，然后entTime=startTime + duration 算得结束时间为 14:00
	public void init() {
		long startTime = m_strategy.next(System.currentTimeMillis());

		startPeriod(startTime);
	}
	//每隔1秒钟会计算是否需要开启一个新的周期
	@Override
	public void run() {
		while (m_active) {
			try {
				long now = System.currentTimeMillis();
				//
				long value = m_strategy.next(now);
				//value>0就开启新的线程
				if (value > 0) {
					startPeriod(value);
				} else if (value < 0) {//value<0的化，会异步开启一个新线程用来结束结束上一个周期
					// last period is over,make it asynchronous
					Threads.forGroup("cat").start(new EndTaskThread(-value));
				}
			} catch (Throwable e) {
				Cat.logError(e);
			}

			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	@Override
	public void shutdown() {
		m_active = false;
	}
	//据起始结束时间来创建 Period 对象，传入分析器的指针。并将周期对象加入到m_periods列表交给PeriodManager管理
	private void startPeriod(long startTime) {
		long endTime = startTime + m_strategy.getDuration();//获得当前周期的结束时间
		Period period = new Period(startTime, endTime, m_analyzerManager, m_serverStateManager, m_logger);
		//周期对象加入到m_periods列表交给PeriodManager管理
		m_periods.add(period);
		//启动period周期，就是开启period下所有周期任务(PeriodTask)线程
		period.start();
	}

	private class EndTaskThread implements Task {

		private long m_startTime;

		public EndTaskThread(long startTime) {
			m_startTime = startTime;
		}

		@Override
		public String getName() {
			return "End-Consumer-Task";
		}

		/***
		 * 移除 m_startTime对应的Period周期
		 */
		@Override
		public void run() {
			endPeriod(m_startTime);
		}

		@Override
		public void shutdown() {
		}
	}
}