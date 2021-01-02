package com.mashibing.idempotence;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerIdempotence {
    public static void main(String[] args) {
        //1.创建kafkaProducer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.31:9092,192.168.0.32:9092,192.168.0.33:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //配置kafka acks以及retires
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //不包含第一次发送, 如果尝试发送3次失败，则系统放弃发送。
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        //将检测超时的时间设置为1ms -- 模拟ack网络超时的场景
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);

        //开启kafka idempotence幂等特性
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        //建议设为1，意为只要有一个发不成功就阻塞在那直到发送成功
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //生产消息
        ProducerRecord<String, String> record = new ProducerRecord<>("topic01", "ack", "test ack");
        producer.send(record);//发送消息给服务器
        producer.flush();//防止kafka发送数据时在本地缓冲

        producer.close();
    }
}
