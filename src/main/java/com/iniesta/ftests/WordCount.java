package com.iniesta.ftests;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {

	public static void main(String[] args) throws Exception {
		
		if(args.length!=2) {
			System.err.println("Input and Output path must be specified");
			System.exit(-1);
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSource<String> text = env.readTextFile(args[0]);
		
		AggregateOperator<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);
		
		counts.writeAsCsv(args[1], "\n", " ");
		
		env.execute("My WordCount example");
	}
	
	public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

	    @Override
	    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
	        // normalize and split the line
	        String[] tokens = value.toLowerCase().split("\\W+");
	        
	        // emit the pairs
	        for (String token : tokens) {
	            if (token.length() > 0) {
	                out.collect(new Tuple2<String, Integer>(token, 1));
	            }   
	        }
	    }
	}
}
