package consumer;

import java.util.Properties;
import java.util.concurrent.Flow.Subscriber;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static java.util.Arrays.asList;

import java.time.Duration;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Properties config = new Properties();
		config.put("bootstrap.servers", "localhost:9092");
		config.put("group.id", "youtube-videos");
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String, String> kc = new KafkaConsumer<>(config);
		kc.subscribe(asList("youtube-videos"));
		
		while(true)
		{
		ConsumerRecords<String, String> records = kc.poll(Duration.ofMillis(100));
		for(ConsumerRecord<String, String> record: records)
			{
				System.out.println(record.value());
			}
		}
		
	}

}
