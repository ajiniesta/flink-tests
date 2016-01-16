package com.iniesta.ftests.stream.taxi;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class AverageTaxiRide {

	public static void main(String[] args) throws Exception {
		
		ParameterTool parameter = ParameterTool.fromArgs(args);
		String input = parameter.getRequired("input");
		int maxDelay = parameter.getInt("maxdelay", 60);
		int servingSpeed = parameter.getInt("speed", 600);
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		DataStreamSource<TaxiRide> rides = env.addSource(new TaxiRideSource(input, maxDelay, servingSpeed));
		
		DataStream<Tuple2<Long, Float>> rideSpeeds = rides.filter(new TaxiRideCleansing.FilterNYC()).keyBy("rideId").flatMap(new AverageCalculator());
		
		rideSpeeds.print();
		
		env.execute("Average Taxi Ride");
	}
	
	public static class AverageCalculator extends RichFlatMapFunction<TaxiRide, Tuple2<Long, Float>>{

		private static final long serialVersionUID = 5132485285041652316L;
		private OperatorState<TaxiRide> state;

		
		@Override
		public void open(Configuration parameters) throws Exception {
			state = this.getRuntimeContext().getKeyValueState("ride", TaxiRide.class, null);
		}

		@Override
		public void flatMap(TaxiRide ride, Collector<Tuple2<Long, Float>> out) throws Exception {

			if(state.value() == null){
				state.update(ride);
			}else{
				TaxiRide startEvent = ride.isStart ? ride : state.value();
				TaxiRide endEvent = ride.isStart ? state.value() : ride;
				
				long timeDiff = endEvent.time.getMillis() - startEvent.time.getMillis();
				float avgSpeed;
				if(timeDiff != 0){
					avgSpeed = (endEvent.travelDistance/timeDiff)*(1000*60*60);
				}else{
					avgSpeed = -1f;
				}
				out.collect(new Tuple2<Long, Float>(startEvent.rideId, avgSpeed));
				state.update(null);
			}
		}
		
	}
}
