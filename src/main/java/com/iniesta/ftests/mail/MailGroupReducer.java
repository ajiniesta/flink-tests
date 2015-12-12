package com.iniesta.ftests.mail;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class MailGroupReducer implements GroupReduceFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {

	private static final long serialVersionUID = 3033555330237857830L;

	@Override
	public void reduce(Iterable<Tuple2<String, String>> mails, Collector<Tuple3<String, String, Integer>> out)
			throws Exception {
		String month = null;
		String email = null;
		int count = 0;
		
		for (Tuple2<String, String> mail : mails) {
			month = mail.f0;
			email = mail.f1;
			count++;
		}
		out.collect(new Tuple3<String, String, Integer>(month, email, count));
	}

}
