package com.iniesta.ftests.tfidf;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class TfIdfComputer implements JoinFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, Tuple3<String, String, Double>> {

	private static final long serialVersionUID = 1603635608208476496L;
	private long nd;

	public TfIdfComputer(long numberOfDocuments) {
		this.nd = numberOfDocuments;
	}

	@Override
	public Tuple3<String, String, Double> join(Tuple2<String, Integer> df, Tuple3<String, String, Integer> tf) throws Exception {
		Double tfidf = tf.f2 * (nd / (double)df.f1);
		return new Tuple3<String, String, Double>(tf.f0, tf.f1, tfidf);
	}

}
