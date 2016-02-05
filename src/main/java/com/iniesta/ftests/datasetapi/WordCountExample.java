package com.iniesta.ftests.datasetapi;

import java.util.StringTokenizer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountExample {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<String> input = env.fromElements(
				"Hello buddy, how are you doing with flink?", 
				"hello yeah pal, good, learning new stuff and you", 
				"Playing with flink.", 
				"Yeah! you rock!");
		
		DataSet<Tuple2<String, Integer>> count = input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			/**
			 * Split by space and emit a tuple with the word and 1
			 */
			@Override
			public void flatMap(String input, Collector<Tuple2<String, Integer>> collector) throws Exception {
				StringTokenizer st = new StringTokenizer(input);
				while (st.hasMoreElements()) {
					String word = (String) st.nextElement();
					collector.collect(new Tuple2<String, Integer>(word, 1));
				}
			}
		}).map(new MapFunction<Tuple2<String,Integer>, Tuple2<String, Integer>>() {
			/**
			 * Cleansing of every word, removing . , ? ! and lowercasing everything
			 */
			@Override
			public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {
				String cleansed = input.f0
						.toLowerCase()
						.replaceAll(",", "")
						.replaceAll("\\.", "")
						.replaceAll("\\?", "")
						.replaceAll("!", "");
				return new Tuple2<String, Integer>(cleansed, input.f1);
			}
		}).groupBy(0).sum(1);
		
		
		count.print();
		
		
	}
	
}
