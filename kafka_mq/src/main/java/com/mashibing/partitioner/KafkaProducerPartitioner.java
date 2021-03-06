package com.mashibing.partitioner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 使用生产者的分区策略
 */
public class KafkaProducerPartitioner {
    public static void main(String[] args) {
        //1.创建kafkaProducer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.31:9092,192.168.0.32:9092,192.168.0.33:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,UserDefinePartitioner.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //生产消息
        for (int i=0; i<6; i++){
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("topic01", "key" + i, "value" + i);
//                    new ProducerRecord<>("topic01", "value" + i);
            producer.send(record);//发送消息给服务器
        }

        producer.close();
    }
}
