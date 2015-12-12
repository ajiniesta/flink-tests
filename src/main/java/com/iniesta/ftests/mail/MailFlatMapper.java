package com.iniesta.ftests.mail;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class MailFlatMapper implements FlatMapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {

	private static final long serialVersionUID = 7990840736888828359L;

	@Override
	public void flatMap(Tuple2<String, String> input, Collector<Tuple3<String, String, Integer>> out) throws Exception {
		String timestamp = input.f0;
		String monthYear = timestamp.substring(0, 7);
		String email = input.f1.substring(input.f1.lastIndexOf("<"), input.f1.length()-1);
		out.collect(new Tuple3<String, String, Integer>(monthYear, email, 1));
		
	}

	

}
