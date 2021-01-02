package com.mashibing.serializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

public class KafkaConsumerUser {
    public static void main(String[] args) {
        //1.创建kafkaProducer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.31:9092,192.168.0.32:9092,192.168.0.33:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDefineDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");

        KafkaConsumer<String, User> consumer = new KafkaConsumer<String, User>(props);

        //订阅相关topic的消息
        consumer.subscribe(Arrays.asList("topic02")); //订阅以topic开头的所有topic

        //遍历消息队列
        while (true){
            ConsumerRecords<String, User> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()){//从队列中取到了数据
                Iterator<ConsumerRecord<String, User>> recordIterator = consumerRecords.iterator();
                while (recordIterator.hasNext()){
                    //获取一个消费消息
                    ConsumerRecord<String, User> record = recordIterator.next();
                    String topic = record.topic();
                    int partition = record.partition();
                    long offset = record.offset();
                    String key = record.key();
                    User value = record.value();
                    long timestamp = record.timestamp();
                    System.out.println(topic+"\t"+partition+","+offset+"\t"+key+" "+value+" "+timestamp);
                }
            }
        }

    }
}