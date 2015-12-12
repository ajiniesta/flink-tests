package com.iniesta.ftests.reply;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class ReplyGraphJob {

	public static void main(String[] args) throws Exception {
		
		if(args.length != 2){
			System.err.println("Please specify the correct arguments: input output");
			System.exit(-1);
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSource<Tuple3<String, String, String>> mails = env.readCsvFile(args[0]).lineDelimiter("##//##").fieldDelimiter("#|#").includeFields("101001").types(String.class, String.class, String.class);
		
		DataSet<Tuple3<String, String, String>> addressMails = mails.filter(new ReplyFilter()).map(new ExtractEmailMap());
		
		DataSet<Tuple2<String, String>> replyConnections = addressMails.join(addressMails).where(2).equalTo(0).projectFirst(1).projectSecond(1);
		
		replyConnections.groupBy(0, 1).reduceGroup(new ConnectionCounter()).writeAsCsv(args[1], "\n", ",");	
		
		env.execute("Reply Graph Job Example");
	}
}
