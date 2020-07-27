package kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class Consumer {
	public static void main(String[] args) {
		String topic = "test";
        String brokers = "node2:9092,node3:9092,node4:9092";
        String StringDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

        Map<String, Object> configC = new HashMap<>();
        configC.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        configC.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer);
        configC.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer);
        configC.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        KafkaConsumer consumer = new KafkaConsumer<String, String>(configC);

        HashSet<String> topics = new HashSet<>();
        topics.add(topic);
        consumer.subscribe(topics);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(200);
            if (!records.isEmpty()) {
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record);
                }
            }
        }
	}

}
