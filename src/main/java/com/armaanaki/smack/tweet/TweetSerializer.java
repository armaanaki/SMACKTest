package com.armaanaki.smack.tweet;

import java.util.Map;

import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

public class TweetSerializer implements Serializer<Tweet>{

	@Override
	public void close() {
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
	}

	@Override
	public byte[] serialize(String arg0, Tweet arg1) {
		byte[] data = SerializationUtils.serialize(arg1);
		return data;
	}
}
