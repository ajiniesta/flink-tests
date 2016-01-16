package com.iniesta.ftests.stream.taxi;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TaxiRideCleansing {

	public static void main(String[] args) throws Exception {
		
		ParameterTool parameter = ParameterTool.fromArgs(args);
		String input = parameter.getRequired("input");
		int maxDelay = parameter.getInt("maxdelay", 60);
		int servingSpeed = parameter.getInt("speed", 600);
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		DataStreamSource<TaxiRide> rides = env.addSource(new TaxiRideSource(input, maxDelay, servingSpeed));
		
		DataStream<TaxiRide> filter = rides.filter(new FilterNYC());
		
		filter.print();
		
		env.execute("Ride Cleansing Exercise");
	}
	
	public static class FilterNYC implements FilterFunction<TaxiRide>{
		private static final long serialVersionUID = 2092293308531389409L;

		@Override
		public boolean filter(TaxiRide ride) throws Exception {				
			return GeoUtils.isInNYC(ride.startLon, ride.startLat) || GeoUtils.isInNYC(ride.endLon, ride.endLat);
		}
	}
}
