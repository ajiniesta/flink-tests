package com.iniesta.ftests.datasetapi;

import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;

public class TotalParser {

	public static int calculateTotal(DataSet<String> input) throws Exception {
		int total = 0;
		List<Integer> list = input.map(new IntParser()).reduce((a, b) -> a + b).collect();
		total = list.get(0);
		return total;
	}

	@SuppressWarnings("serial")
	public static class IntParser extends RichMapFunction<String, Integer> {

		@Override
		public Integer map(String input) throws Exception {
			int value = 0;
			try {
				value = Integer.parseInt(input);
			} catch (NumberFormatException e) {
			}
			return value;
		}

	}
}
