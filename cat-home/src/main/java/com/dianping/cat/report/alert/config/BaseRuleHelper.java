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
package com.dianping.cat.report.alert.config;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.dianping.cat.Cat;
import com.dianping.cat.alarm.rule.entity.Condition;
import com.dianping.cat.alarm.rule.entity.Config;

@Named
public class BaseRuleHelper {
	/***
	 * 循环遍历处理Config规则配置
	 * @param configs
	 * @return
	 * <monitor-rules>
	 *    <rule id="cat;URL;URL.Method;count" available="true">
	 *       <config starttime="00:00" endtime="24:00">
	 *          <condition minute="1" alertType="warning">
	 *             <sub-condition type="MaxVal" text="3000"/>
	 *          </condition>
	 *       </config>
	 *    </rule>
	 *    <rule id="cat;URL;URL.Method;failRatio" available="false">
	 *       <config starttime="00:00" endtime="24:00">
	 *          <condition minute="1" alertType="warning">
	 *             <sub-condition type="MaxVal" text="0.1"/>
	 *          </condition>
	 *       </config>
	 *    </rule>
	 * </monitor-rules>
	 */
	public Pair<Integer, List<Condition>> convertConditions(List<Config> configs) {
		int maxMinute = 0;
		List<Condition> conditions = new ArrayList<Condition>();
		Iterator<Config> iterator = configs.iterator();
		//遍历config,获得所有的condition中配置的最大的miniute
		while (iterator.hasNext()) {
			Config config = iterator.next();
			//检查当前时间是否在配置的时间范围内
			if (checkTimeValidate(config)) {
				//获得所有的condition配置
				List<Condition> tmpConditions = config.getConditions();
				conditions.addAll(tmpConditions);
				//遍历condition,并获得condition的minute配置
				for (Condition con : tmpConditions) {
					int tmpMinute = con.getMinute();

					if (tmpMinute > maxMinute) {
						maxMinute = tmpMinute;
					}
				}
			}
		}

		if (maxMinute > 0) {
			return new Pair<Integer, List<Condition>>(maxMinute, conditions);
		} else {
			return null;
		}
	}

	private boolean checkTimeValidate(Config config) {
		try {
			if (compareTime(config.getStarttime(), config.getEndtime())) {
				return true;
			} else {
				return false;
			}
		} catch (Exception ex) {
			Cat.logError("throw exception when judge time: " + config.toString(), ex);
			return false;
		}
	}

	/***
	 * 检查当前时间是否在配置的时间范围内
	 * @param start 开始时间
	 * @param end 结束时间
	 * @return
	 */
	private boolean compareTime(String start, String end) {
		String[] startTime = start.split(":");
		int hourStart = Integer.parseInt(startTime[0]);//小时
		int minuteStart = Integer.parseInt(startTime[1]);//分钟
		int startMinute = hourStart * 60 + minuteStart;//开始时间转换成分

		String[] endTime = end.split(":");
		int hourEnd = Integer.parseInt(endTime[0]);
		int minuteEnd = Integer.parseInt(endTime[1]);
		int endMinute = hourEnd * 60 + minuteEnd;//结束时间转换成分

		Calendar cal = Calendar.getInstance();//当前时间
		int current = cal.get(Calendar.HOUR_OF_DAY) * 60 + cal.get(Calendar.MINUTE);

		return current >= startMinute && current <= endMinute;
	}
}
