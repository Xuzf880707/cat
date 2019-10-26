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

/***
 * 不会卡在整点时间，例如10:00去开启或结束一个周期，因为周期创建是需要消耗一定时间，
 * 这样可以避免消息过来周期对象还未创建好，或者消息还没有处理完，就要去结束周期
 */
public class PeriodStrategy {
	private long m_duration;//默认是1小时
	//提前启动一个周期的时间，默认是3分钟
	private long m_extraTime;
	//延迟结束一个周期的时间，默认是3分钟
	private long m_aheadTime;

	private long m_lastStartTime;

	private long m_lastEndTime;

	public PeriodStrategy(long duration, long extraTime, long aheadTime) {
		m_duration = duration;//60分钟
		m_extraTime = extraTime;//3分钟
		m_aheadTime = aheadTime;//3分钟
		m_lastStartTime = -1;
		m_lastEndTime = 0;
	}

	public long getDuration() {
		return m_duration;
	}
	//个周期的开始/结束会参考PeriodStrategy的计算结果，参数duration为每个周期的长度，默认是1个小时，而且周期是整点时段，例如：1:00-2:00, 2:00-3:00。
	public long next(long now) {
		//比如当前时间是 11:47.123，那么startTime就是 11:00.000
		long startTime = now - now % m_duration;//获得当前周期的开始时间

		//如果startTime大于上次周期启动时间,说明应该开启新的周期
		if (startTime > m_lastStartTime) {//
			m_lastStartTime = startTime;
			return startTime;
		}

		// 判断当前时间比起上次周期启动时间是不是已经过了 57 分钟(duration - aheadTime )，即提前3分钟启动下一个周期。
		if (now - m_lastStartTime >= m_duration - m_aheadTime) {
			m_lastStartTime = startTime + m_duration;
			return startTime + m_duration;
		}

		// 如果上面if还未执行，我们则认为当前周期已经被启动，那么会判断是否需要结束当前周期，
		// 即当前时间比起上次周期启动时间是不是已经过了 63 分钟(duration + extraTime)，即延迟3分钟关闭上一个周期。
		if (now - m_lastEndTime >= m_duration + m_extraTime) {
			long lastEndTime = m_lastEndTime;
			m_lastEndTime = startTime;
			return -lastEndTime;
		}

		return 0;
	}
}