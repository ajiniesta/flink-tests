package com.iniesta.ftests.mail;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

public class MailExample {

	public static void main(String[] args) throws Exception {
		
		if(args.length!=3){
			System.err.println("You have to input the gzip file with the mails as an argument!");
			System.exit(-1);
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple2<String, String>> timestampSender = env.readCsvFile(args[0]).lineDelimiter("##//##").fieldDelimiter("#|#").includeFields("011")
				.types(String.class, String.class);

		if("flatMap".equalsIgnoreCase(args[2])){
			timestampSender.flatMap(new MailFlatMapper()).groupBy(0,1).sum(2).writeAsCsv(args[1], "\n", ",");
		}else {
			timestampSender.map(new MailMapper()).groupBy(0, 1).reduceGroup(new MailGroupReducer()).writeAsCsv(args[1], "\n", ",");
		}
		
		
		
		env.execute("Count Mail");
	}
	
	
}
