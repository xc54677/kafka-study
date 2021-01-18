package com.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest(classes = KafkaSpringBootApplication.class)
@RunWith(SpringRunner.class)
public class KafkaTemplateTests {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 非事务下执行
     * 执行前要将以下配置注释，不然会报错
     * spring.kafka.producer.transaction-id-prefix=transaction-id-
     */
    @Test
    public void testSendMessage01(){
        kafkaTemplate.send(new ProducerRecord<>("topic02", "001", "This is Kafka Template"));
    }

    /**
     * 事务下执行
     * 执行前要将以下配置以下的配置
     * spring.kafka.producer.transaction-id-prefix=transaction-id-
     */
    @Test
    public void testSendMessage02(){ //事务下执行
        kafkaTemplate.executeInTransaction(
                kafkaOperations -> {
                    kafkaOperations.send(new ProducerRecord<>("topic02", "001", "This is Kafka transaction");
                    return null;
                }
        );
    }

}
