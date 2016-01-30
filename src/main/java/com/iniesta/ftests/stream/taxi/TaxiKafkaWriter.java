package com.iniesta.ftests.stream.taxi;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import com.iniesta.ftests.stream.taxi.TaxiRideCleansing.FilterNYC;

public class TaxiKafkaWriter {

	public static void main(String[] args) throws Exception {
		ParameterTool parameter = ParameterTool.fromArgs(args);
		String input = parameter.getRequired("input");
		int maxDelay = parameter.getInt("maxdelay", 60);
		int servingSpeed = parameter.getInt("speed", 600);
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		DataStreamSource<TaxiRide> rides = env.addSource(new TaxiRideSource(input, maxDelay, servingSpeed));
		
		DataStream<TaxiRide> filter = rides.filter(new FilterNYC());
		
		
		filter.addSink(new FlinkKafkaProducer<>("localhost:9092", "cleansedRides", new TaxiRideSchema()));
		env.execute("Taxi Kafka Writer");
	}
}
