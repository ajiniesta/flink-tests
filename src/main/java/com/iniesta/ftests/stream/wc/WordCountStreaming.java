package com.iniesta.ftests.stream.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WordCountStreaming {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		DataStream<Tuple2<String, Integer>> ds = env.socketTextStream("localhost", 9999).flatMap(new Splitter()).keyBy(0).timeWindow(Time.minutes(1)).sum(1);
		
		ds.print();
		
		env.execute("WordCount in Streaming");
	}
}
