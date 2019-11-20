/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jd.kafka;

import com.jd.kafka.CustomProcessingTimeTrigger;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class TriggerStreamingJob {
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		// get input data
		DataStream<Tuple2<String, Integer>> res_keyBy =
				env.socketTextStream("localhost",9999)
				//map是将每一行单词变为一个tuple2
				.flatMap(new Splitter())
				//keyBy进行分区，按照第一列，也就是按照单词进行分区
				.keyBy(0)
				//指定窗口，每10秒个计算一次
				.window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
				.trigger(new CustomProcessingTimeTrigger())
				//计算个数，计算第1列
				.sum(1);
		res_keyBy.print();
		env.execute();
	}

	public static class Splitter implements FlatMapFunction<String,Tuple2<String,Integer>>{

		@Override
		public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
			for (String word: s.split("\\W+")){
				collector.collect(new Tuple2<String,Integer>(word,1));
			}
		}
	}

}

