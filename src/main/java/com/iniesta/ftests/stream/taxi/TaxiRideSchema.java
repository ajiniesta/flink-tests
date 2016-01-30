package com.iniesta.ftests.stream.taxi;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class TaxiRideSchema implements DeserializationSchema<TaxiRide>, SerializationSchema<TaxiRide, byte[]> {

	private static final long serialVersionUID = 8277498855936176576L;

	@Override
	public byte[] serialize(TaxiRide element) {
		return element.toString().getBytes();
	}

	@Override
	public TaxiRide deserialize(byte[] message) {
		return TaxiRide.fromString(new String(message));
	}

	@Override
	public boolean isEndOfStream(TaxiRide nextElement) {
		return false;
	}

	@Override
	public TypeInformation<TaxiRide> getProducedType() {
		return TypeExtractor.getForClass(TaxiRide.class);
	}
}