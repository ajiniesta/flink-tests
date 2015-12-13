package com.iniesta.ftests.tfidf;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class TfIdfJob {

	public final static String[] STOP_WORDS = {
			"the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is",
			"there", "it", "this", "that", "on", "was", "by", "of", "to", "in",
			"to", "message", "not", "be", "with", "you", "have", "as", "can"
	};

	
	public static void main(String[] args) throws Exception {
		if(args.length!=2){
			System.err.println("Please specify the correct arguments: input output");
			System.exit(-1);
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSource<Tuple2<String, String>> mails = env.readCsvFile(args[0]).lineDelimiter("##//##").fieldDelimiter("#|#").includeFields("10001").types(String.class, String.class);

		long numberOfDocuments = mails.count();
	
		DataSet<Tuple2<String, Integer>> docFrec = mails.flatMap(new UniqueWordExtractor(STOP_WORDS)).groupBy(0).sum(1);
		
		DataSet<Tuple3<String, String, Integer>> termFrec = mails.flatMap(new TFComputes(STOP_WORDS));
		
		DataSet<Tuple3<String, String, Double>> tfIdf = docFrec.join(termFrec).where(0).equalTo(1).with(new TfIdfComputer(numberOfDocuments));
		
		tfIdf.writeAsCsv(args[1], "\n", ",");
		
		env.execute("TF-IDF Job");
	}
}
