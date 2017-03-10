package com.armaanaki.smack.kafka;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.armaanaki.smack.tweet.*;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;

public class KafkaStream {
	public static void main(String[] args) {
		//final ActorSystem system = ActorSystem.create("actorSystem");
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.194.194:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "com.armaanaki.smack.tweet.TweetSerializer");
		
		final KafkaProducer<String, Tweet> kafkaProducer = new KafkaProducer<String, Tweet>(props);
		
		/*CompletionStage<Done> done = (CompletionStage<Done>) Source.range(1, 100)
			.map(n -> n.toString()).map(elem -> new ProducerRecord<String, Tweet>("topic", elem))
			.runWith(Producer.plainSink(producerSettings, kafkaProducer), materializer);*/
		for (int i = 0; i < 1000; i++) {
			ProducerRecord<String, Tweet> record = new ProducerRecord<String, Tweet>("tweets1", Integer.toString(i), new Tweet(new Date(), "Test" + i));
			kafkaProducer.send(record);
		}
		kafkaProducer.close();
	}
}