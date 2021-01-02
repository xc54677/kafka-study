package com.mashibing.dml;

import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaTopicDML {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //1.创建 KafkaAdminClient
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.31:9092,192.168.0.32:9092,192.168.0.33:9092");
        KafkaAdminClient adminClient = (KafkaAdminClient) KafkaAdminClient.create(props);

        //创建topic信息
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(new NewTopic("topic02", 3, (short) 3)));
        createTopicsResult.all().get();//上一步的创建topic时属于异步，所以这一步是为了将异步创建变成同步创建了

        //查看topic列表
//        ListTopicsResult topicsResult = adminClient.listTopics();
//        Set<String> names = topicsResult.names().get();
//        for (String name : names) {
//            System.out.println(name);
//        }

        //删除topic
//        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList("Topico2", "Topic03"));
//        deleteTopicsResult.all().get();//同步删除

        //查看topic详细信息
//        DescribeTopicsResult dtr = adminClient.describeTopics(Arrays.asList("topic01"));
//        Map<String, TopicDescription> topicDescriptionMap = dtr.all().get();
//        for (Map.Entry<String, TopicDescription> entry : topicDescriptionMap.entrySet()) {
//            System.out.println(entry.getKey()+"\t"+entry.getValue());
//        }

        //关闭AdminClient
        adminClient.close();
    }

}
