package com.armaanaki.smack;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;

public class SparkToCassandra {
	public static void main(String[] args) throws Exception{
		if (args.length < 4) {
			System.err.println("Usage: SparkTest <kafka brokers> <topics> <cassandra host> <cassandra port>");
		    System.exit(1);
		}
		
		String brokers = args[0];
		String topics = args[1];
		
		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		
		SparkConf sparkConf = new SparkConf().setAppName("SparkTest")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") 
				.set("spark.cassandra.connection.host", args[2])
				.set("spark.cassandra.connection.port", args[3]);
		
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		
		 Map <String, Object> kafkaParams = new HashMap<>();
		 kafkaParams.put("bootstrap.servers", brokers);
		 kafkaParams.put("key.deserializer", StringDeserializer.class);
		 kafkaParams.put("value.deserializer", TweetDeserializer.class);
		 kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		 
		 JavaInputDStream<ConsumerRecord<String, Tweet>> tweets = KafkaUtils.createDirectStream(
				 jssc, 
				 LocationStrategies.PreferConsistent(),
				 ConsumerStrategies.<String, Tweet>Subscribe(topicsSet, kafkaParams)
				);
		 
		 
		JavaDStream<Tweet> tweetStream= tweets.map(
				 new Function<ConsumerRecord<String, Tweet>, Tweet>() {
					 @Override
					 public Tweet call(ConsumerRecord<String, Tweet> record) {
						 return record.value();
					 }
				 });
		tweetStream.print();
		CassandraStreamingJavaUtil.javaFunctions(tweetStream).writerBuilder("dcos", "tweets", CassandraJavaUtil.mapToRow(Tweet.class)).saveToCassandra();
		jssc.start();
		jssc.awaitTermination();
	}
}
