package com.iniesta.ftests.reply;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class ReplyFilter implements FilterFunction<Tuple3<String, String, String>> {

	private static final long serialVersionUID = -129962322065000844L;

	@Override
	public boolean filter(Tuple3<String, String, String> input) throws Exception {
		return !(input.f1.contains("<jira@apache.org>") || input.f1.contains("<git@git.apache.org>") || input.f2.startsWith("<JIRA."));
		
	}

}
