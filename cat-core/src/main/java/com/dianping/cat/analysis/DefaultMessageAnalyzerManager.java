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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Named;

import com.dianping.cat.Cat;
import com.dianping.cat.config.server.ServerConfigManager;

@Named(type = MessageAnalyzerManager.class)
public class DefaultMessageAnalyzerManager extends ContainerHolder
						implements MessageAnalyzerManager, Initializable,	LogEnabled {
	private static final long MINUTE = 60 * 1000L;

	protected Logger m_logger;

	private long m_duration = 60 * MINUTE;

	private long m_extraTime = 3 * MINUTE;

	private List<String> m_analyzerNames;
	//最外层Map的key的类型为long，代表由startTime对应的周期
	//value还是一个Map，Map的key类型是String，是分析器的名字，代表一类分析器，
	// value是MessageAnalyzer列表，同一类分析器，至少有一个MessageAnalyzer实例，
	// 对于复杂耗时的分析任务，我们通常会开启更多的实例处理。
	private Map<Long, Map<String, List<MessageAnalyzer>>> m_analyzers = new HashMap<Long, Map<String, List<MessageAnalyzer>>>();
	//根据分析器名字和周期时间，获取当前周期、该类别分析器所有实例
	@Override
	public List<MessageAnalyzer> getAnalyzer(String name, long startTime) {
		// remove last two hour analyzer
		try {//移除2个小时前的的分析器
			Map<String, List<MessageAnalyzer>> temp = m_analyzers.remove(startTime - m_duration * 2);

			if (temp != null) {
				for (List<MessageAnalyzer> anlyzers : temp.values()) {
					for (MessageAnalyzer analyzer : anlyzers) {
						analyzer.destroy();
					}
				}
			}
		} catch (Exception e) {
			Cat.logError(e);
		}
		//获得当前周期的分析器集合， key分析器名字，value是分析器对应PeriodTask任务
		Map<String, List<MessageAnalyzer>> map = m_analyzers.get(startTime);

		if (map == null) {
			synchronized (m_analyzers) {
				map = m_analyzers.get(startTime);

				if (map == null) {
					map = new HashMap<String, List<MessageAnalyzer>>();
					m_analyzers.put(startTime, map);
				}
			}
		}
		//从当前周期的分析器集合中根据分析名称找出对应的分析器列表，一个分析器名称可能对应多个分析器
		List<MessageAnalyzer> analyzers = map.get(name);
		//当前周期不存在对应分析器的PeriodTask
		if (analyzers == null) {
			synchronized (map) {
				analyzers = map.get(name);

				if (analyzers == null) {
					analyzers = new ArrayList<MessageAnalyzer>();
					//根据分析器名称获得对应的分析器
					MessageAnalyzer analyzer = lookup(MessageAnalyzer.class, name);

					analyzer.setIndex(0);
					analyzer.initialize(startTime, m_duration, m_extraTime);
					analyzers.add(analyzer);
					//根据name查询服务器允许配置的MessageAnalyzer个数，所以一个周期是允许同名的多个分析器
					int count = analyzer.getAnanlyzerCount(name);

					for (int i = 1; i < count; i++) {
						MessageAnalyzer tempAnalyzer = lookup(MessageAnalyzer.class, name);

						tempAnalyzer.setIndex(i);
						tempAnalyzer.initialize(startTime, m_duration, m_extraTime);
						analyzers.add(tempAnalyzer);
					}
					map.put(name, analyzers);
				}
			}
		}

		return analyzers;
	}

	@Override
	public List<String> getAnalyzerNames() {
		return m_analyzerNames;
	}

	@Override
	public void initialize() throws InitializationException {
		//初始化所有的分析管理器，每个名字对应一类分析器
		Map<String, MessageAnalyzer> map = lookupMap(MessageAnalyzer.class);

		for (MessageAnalyzer analyzer : map.values()) {
			analyzer.destroy();
		}

		m_analyzerNames = new ArrayList<String>(map.keySet());

		Collections.sort(m_analyzerNames, new Comparator<String>() {
			@Override
			public int compare(String str1, String str2) {
				String state = "state";
				String top = "top";

				if (state.equals(str1)) {
					return 1;
				} else if (state.equals(str2)) {
					return -1;
				}
				if (top.equals(str1)) {
					return -1;
				} else if (top.equals(str2)) {
					return 1;
				}
				return str1.compareTo(str2);
			}
		});

		ServerConfigManager manager = lookup(ServerConfigManager.class);
		List<String> disables = new ArrayList<String>();

		for (String name : m_analyzerNames) {

			if (!manager.getEnableOfRealtimeAnalyzer(name)) {
				disables.add(name);
			}
		}
		for (String name : disables) {
			m_analyzerNames.remove(name);
		}
	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}
}
