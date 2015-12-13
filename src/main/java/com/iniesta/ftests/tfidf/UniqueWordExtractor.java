package com.iniesta.ftests.tfidf;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class UniqueWordExtractor extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>> {

	private static final long serialVersionUID = -6060414437874814343L;

	private Set<String> stopWords;
	private transient Set<String> emittedWords;
	private transient Pattern wordPattern;
	
	public UniqueWordExtractor(String[] stopWords) {
		this.stopWords = new HashSet<>(Arrays.asList(stopWords));
	}
		
	@Override
	public void open(Configuration parameters) throws Exception {
		this.emittedWords = new HashSet<String>();
		this.wordPattern = Pattern.compile("(\\p{Alpha})+");
	}

	@Override
	public void flatMap(Tuple2<String, String> input, Collector<Tuple2<String, Integer>> output) throws Exception {
		
		this.emittedWords.clear();
		
		StringTokenizer st = new StringTokenizer(input.f1);
		
		while (st.hasMoreTokens()) {
			String word = st.nextToken().toLowerCase();
			Matcher m = this.wordPattern.matcher(word);
			if(m.matches() && !this.stopWords.contains(word) && !this.emittedWords.contains(word)){
				output.collect(new Tuple2<String, Integer>(word, 1));
				this.emittedWords.add(word);
			}
		}
		
	}

}
