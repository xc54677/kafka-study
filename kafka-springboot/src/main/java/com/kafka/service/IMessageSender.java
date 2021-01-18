package com.kafka.service;

public interface IMessageSender {
    void sendMessage(String topicName, String key, String message);
}
