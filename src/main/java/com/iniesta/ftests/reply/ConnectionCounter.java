package com.iniesta.ftests.reply;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class ConnectionCounter implements GroupReduceFunction<Tuple2<String,String>, Tuple3<String, String, Integer>>  {

	private static final long serialVersionUID = -2201407848701486482L;

	@Override
	public void reduce(Iterable<Tuple2<String, String>> input, Collector<Tuple3<String, String, Integer>> out)
			throws Exception {
		String a = null;
		String b = null;
		int count = 0;
		for (Tuple2<String, String> tuple : input) {
			a = tuple.f0;
			b = tuple.f1;
			count ++;
		}
		out.collect(new Tuple3<String, String, Integer>(a, b, count));
		
	}



}
