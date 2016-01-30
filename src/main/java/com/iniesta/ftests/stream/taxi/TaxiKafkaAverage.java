package com.iniesta.ftests.stream.taxi;

import java.util.Properties;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;

import com.iniesta.ftests.stream.taxi.AverageTaxiRide.AverageCalculator;

public class TaxiKafkaAverage {

	protected final static int maxEventDelay = 60;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		ParameterTool parameter = ParameterTool.fromArgs(args);
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		Properties props = new Properties();
		props.setProperty("zookeeper.connect", parameter.get("zookeeper.connect", "localhost:2181"));
		props.setProperty("bootstrap.servers", parameter.get("bootstrap.servers", "localhost:9092"));
		props.setProperty("group.id", parameter.get("group.id", "group1"));
				
		DataStream<TaxiRide> rides = env.addSource(new FlinkKafkaConsumer082<>("cleansedRides", new TaxiRideSchema(), props))
				.assignTimestamps(new TimestampExtractor<TaxiRide>() {
					
					long current;
					
					@Override
					public long getCurrentWatermark() {
						return current;
					}
					
					@Override
					public long extractWatermark(TaxiRide ride, long currentTimestamp) {
						current =  current - (maxEventDelay*1000);
						return -1;
					}
						
					@Override
					public long extractTimestamp(TaxiRide ride, long currentTimestamp) {
						return ride.time.getMillis();
					}
				});
		
		DataStream<Tuple2<Long, Float>> rideSpeeds = rides.keyBy("rideId").flatMap(new AverageCalculator());
		
		rideSpeeds.print();
		
		env.execute("Average Taxi Ride");
	}
}
