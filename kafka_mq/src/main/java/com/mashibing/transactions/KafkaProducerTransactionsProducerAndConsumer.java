package com.mashibing.transactions;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class KafkaProducerTransactionsProducerAndConsumer {
    public static void main(String[] args) {

        KafkaProducer<String, String> producer = buildKafkaProducer();
        KafkaConsumer<String, String> consumer = buildKafkaConsumer("g1");

        //初始化事务
        producer.initTransactions();

        consumer.subscribe(Arrays.asList("topic01"));

        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()){
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

                Iterator<ConsumerRecord<String, String>> recordIterator = consumerRecords.iterator();

                //开启事务
                producer.beginTransaction();
                try {
                    //迭代数据，进行业务处理
                    while (recordIterator.hasNext()){
                        ConsumerRecord<String, String> record = recordIterator.next();
                        //存储元数据
                        offsets.put(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset()+1));
                        ProducerRecord<String, String> pRecord = new ProducerRecord<>("topic02", record.key(),
                                record.value() + "mashibing edu online");
                        producer.send(pRecord);
                    }
                    //提交事务
                    producer.sendOffsetsToTransaction(offsets, "g1");
                    producer.commitTransaction();
                }catch (Exception e){
                    System.err.println("错误了: " + e.getMessage());
                    //终止事务
                    producer.abortTransaction();
                }
            }
        }


    }

    public static KafkaProducer<String, String> buildKafkaProducer(){
        //1.创建kafkaProducer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.31:9092,192.168.0.32:9092,192.168.0.33:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //必须配置事务ID，且id必须是唯一的
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id"+ UUID.randomUUID().toString());
        //配置kafka批处理大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        //等待5毫秒，如果在5毫秒内达到1024条数据，就发送，如果5毫秒内达不到1024条，就直接发送
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        //配置kafka重试机制和幂等性
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //设置服务端接收消息应答时间20s
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);


        return new KafkaProducer<String, String>(props);
    }

    public static KafkaConsumer<String, String> buildKafkaConsumer(String groupId){
        //1.创建kafkaProducer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.31:9092,192.168.0.32:9092,192.168.0.33:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        //设置消费者的消费事务的隔离级别为read_uncommitted (默认是read_uncommitted)
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        //必须关闭消费者端的 offset自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<String, String>(props);
    }
}
