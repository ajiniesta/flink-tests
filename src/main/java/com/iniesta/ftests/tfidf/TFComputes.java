package com.iniesta.ftests.tfidf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class TFComputes extends RichFlatMapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {

	private static final long serialVersionUID = 4755402117325001711L;
	
	// set of stop words
	private Set<String> stopWords;
	// map to count the frequency of words
	private transient Map<String, Integer> wordCounts;
	// pattern to match against words
	private transient Pattern wordPattern;

	public TFComputes(String[] stopWords) {
		this.stopWords = new HashSet<>(Arrays.asList(stopWords));
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.wordPattern = Pattern.compile("(\\p{Alpha})+");
		this.wordCounts = new HashMap<String, Integer>();
	}

	@Override
	public void flatMap(Tuple2<String, String> input, Collector<Tuple3<String, String, Integer>> output) throws Exception {
		this.wordCounts.clear();
			
		StringTokenizer st = new StringTokenizer(input.f1);
		
		while (st.hasMoreTokens()) {
			String word = st.nextToken().toLowerCase();
			Matcher m = this.wordPattern.matcher(word);
			if(m.matches() && !this.stopWords.contains(word)){
				int count = 0;
				if(wordCounts.containsKey(word)){
					count = wordCounts.get(word);
				}
				wordCounts.put(word, count+1);
			}
		}
		
		for (String word : this.wordCounts.keySet()) {
			output.collect(new Tuple3<String, String, Integer>(input.f0, word, wordCounts.get(word)));
		}
	}

}
