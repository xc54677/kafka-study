package com.kafka;

import com.kafka.service.IMessageSender;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest(classes = KafkaSpringBootApplication.class)
@RunWith(SpringRunner.class)
public class MessageSenderTest {
    @Autowired
    private IMessageSender messageSender;

    @Test
    public void testSendMessage(){
        messageSender.sendMessage("topic02", "001", "This is Kafka sender test");
    }
}
