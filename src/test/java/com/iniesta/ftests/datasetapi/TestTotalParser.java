package com.iniesta.ftests.datasetapi;

import static org.junit.Assert.assertEquals;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTotalParser {

	private static ExecutionEnvironment env;
	@BeforeClass
	public static void before(){
		env = ExecutionEnvironment.getExecutionEnvironment();
	}
	
	@Test
	public void testRegularWorking() throws Exception {
		DataSet<String> elements = env.fromElements("1","2","3");
		int total = TotalParser.calculateTotal(elements);
		assertEquals(6, total);
	}

	@Test
	public void testAvoidingLetters() throws Exception {
		DataSet<String> elements = env.fromElements("1","2","3","f");
		int total = TotalParser.calculateTotal(elements);
		assertEquals(6, total);
	}
}
