package com.iniesta.ftests.stream.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

	private static final long serialVersionUID = 3358133205277244604L;

	@Override
	public void flatMap(String input, Collector<Tuple2<String, Integer>> output) throws Exception {
		String[] tokens = input.toLowerCase().split("\\W+");
		for (String token : tokens) {
			if(!token.isEmpty()){
				output.collect(new Tuple2<String, Integer>(token, 1));
			}
		}
	}

}
