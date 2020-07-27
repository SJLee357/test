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

public class Producer {
	public static void main(String[] args) {
		String topic = "test";
        String brokers = "node2:9092,node3:9092,node4:9092";
        String StringSerializer = "org.apache.kafka.common.serialization.StringSerializer";


        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer);

        KafkaProducer producer = new KafkaProducer<String, String>(config);

        while(true) {
        	String msg = "";
        	InetAddress local;
			try {
				local = InetAddress.getLocalHost();
				String ip = local.getHostAddress();
				SimpleDateFormat format = new SimpleDateFormat ( "yyyyMMddHHmmss");
				Date time = new Date();
				String sTime = format.format(time);
	            msg = sTime + " " + ip;
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			System.out.println(msg);
            ProducerRecord < String, String > record = new ProducerRecord < String, String > ( topic, msg);
			producer.send(record);
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
	}
}
