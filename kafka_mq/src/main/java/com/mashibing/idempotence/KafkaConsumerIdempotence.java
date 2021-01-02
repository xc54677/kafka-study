package com.mashibing.idempotence;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class KafkaConsumerIdempotence {
    public static void main(String[] args) {
        //1.创建kafkaProducer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.31:9092,192.168.0.32:9092,192.168.0.33:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g4");

        //earliest, 如果系统没有消费者的偏移量，系统会读取该分区最早的偏移量
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // offset偏移量自动提交 - 默认为true, false - 关闭自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //订阅相关topic的消息
        consumer.subscribe(Arrays.asList("topic01")); //订阅以topic开头的所有topic

        //遍历消息队列
        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()){//从队列中取到了数据
                Iterator<ConsumerRecord<String, String>> recordIterator = consumerRecords.iterator();

                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

                while (recordIterator.hasNext()){
                    //获取一个消费消息
                    ConsumerRecord<String, String> record = recordIterator.next();
                    String topic = record.topic();
                    int partition = record.partition();
                    long offset = record.offset();
                    String key = record.key();
                    String value = record.value();
                    long timestamp = record.timestamp();

                    //记录消费分区的偏移量元数据，一定要在提交的时候offset+1
                    offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset+1));
                    //异步提交消费者偏移量
                    consumer.commitAsync(offsets, new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            System.out.println("offsets:" + offsets + "\t" + ",exception: " + exception);
                        }
                    });

                    System.out.println(topic+"\t"+partition+","+offset+"\t"+key+" "+value+" "+timestamp);
                }
            }
        }

    }
}
