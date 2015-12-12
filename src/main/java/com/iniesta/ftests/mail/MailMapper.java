package com.iniesta.ftests.mail;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class MailMapper implements MapFunction<Tuple2<String, String>, Tuple2<String, String>> {

	private static final long serialVersionUID = -1323167718641455883L;

	@Override
	public Tuple2<String, String> map(Tuple2<String, String> input) throws Exception {
		Tuple2<String, String> output = new Tuple2<>();
		String timestamp = input.f0;
		String monthYear = timestamp.substring(0, 7);
		String email = input.f1.substring(input.f1.lastIndexOf("<"), input.f1.length()-1);
		output.f0 = monthYear;
		output.f1 = email;
		return output;
	}

}
