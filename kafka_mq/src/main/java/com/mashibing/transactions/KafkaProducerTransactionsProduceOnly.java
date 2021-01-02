package com.mashibing.transactions;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class KafkaProducerTransactionsProduceOnly {
    public static void main(String[] args) {

        KafkaProducer<String, String> producer = buildKafkaProducer();

        //初始化事务
        producer.initTransactions();

        //生产消息
        try {
            //开始事务
            producer.beginTransaction();
            for (int i=0; i<10; i++){
                if (i == 8){
                    int j = 10/0;
                }
                ProducerRecord<String, String> record = new ProducerRecord<>("topic01",
                        "transaction" + i, "error data" + i);
                producer.send(record);//发送消息给服务器

                producer.flush();
            }
            //提交事务
            producer.commitTransaction();
        }catch (Exception e){
            e.printStackTrace();
            //终止事务
            producer.abortTransaction();
        }finally {
            producer.close();
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
}
