package com.mashibing.serializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Date;
import java.util.Properties;

public class KafkaProducerUser {
    public static void main(String[] args) {
        //1.创建kafkaProducer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.31:9092,192.168.0.32:9092,192.168.0.33:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserDefineSerializer.class.getName());
        KafkaProducer<String, User> producer = new KafkaProducer<String, User>(props);

        //生产消息
        for (int i=0; i<10; i++){
            ProducerRecord<String, User> record = new ProducerRecord<>("topic02", "key" + i,
                    new User(i, "user"+i, new Date()));
            producer.send(record);//发送消息给服务器
        }

        producer.close();
    }
}
