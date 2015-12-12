package com.iniesta.ftests.reply;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class ExtractEmailMap implements MapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>> {

	private static final long serialVersionUID = 1247763138902723180L;

	@Override
	public Tuple3<String, String, String> map(Tuple3<String, String, String> input) throws Exception {
		String email = input.f1.substring(input.f1.lastIndexOf("<")+1, input.f1.length()-1);
		return new Tuple3<String, String, String>(input.f0, email, input.f2);
	}

}
