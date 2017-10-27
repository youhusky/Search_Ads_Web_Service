package kafka;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer1 {
	Properties props = new Properties();
	ProducerConfig config;
	Producer<String, String> producer;
	
	public KafkaProducer1() {
        props.put("metadata.broker.list", "34.201.48.237:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("partitioner.class", "main.java.SimplePartitioner");
        props.put("request.required.acks", "1");
        config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
        

	}
	
	public void SendMessage(String topic, String message) {
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, message);
		producer.send(data);
		System.out.println("send kafka");		
	}
	
}
