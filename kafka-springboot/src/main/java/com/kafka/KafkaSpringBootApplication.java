package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListeners;
import org.springframework.messaging.handler.annotation.SendTo;

import java.io.IOException;

@SpringBootApplication
public class KafkaSpringBootApplication {
    public static void main(String[] args) throws IOException {
        SpringApplication.run(KafkaSpringBootApplication.class,args);
        System.in.read();
    }

    /**
     * 接收kafka消息
     * @param record
     */
    @KafkaListeners(
            value = {
                    @KafkaListener(topics = {"topic01"})
            }
    )
    public void receive01(ConsumerRecord<String, String> record){
        System.out.println("record: " + record);
    }


    /**
     * 接收kafka topic02的消息，并将接收到的消息处理下再发给topic03
     * @param record
     */
    @KafkaListeners(
            value = {
                    @KafkaListener(topics = {"topic02"})
            }
    )
    @SendTo("topic03")
    public String receive02(ConsumerRecord<String, String> record){
        return record.value() + "/t" + "study";
    }
}
