package com.mashibing.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class UserDefinePartitioner implements Partitioner {
    private AtomicInteger counter = new AtomicInteger(0);

    /**
     * 返回值是返回的分区号
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return //分区号
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //获取所有分区数
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if (keyBytes == null){
            int andIncrement = counter.getAndIncrement();
            return (andIncrement & Integer.MAX_VALUE) % numPartitions;
        }else {
            // hash the keyBytes to choose a partition
            // Utils.murmur2 --> 对key取hash值
            // Utils.toPositive --> 将值取正
            System.out.println("Utils.murmur2(keyBytes): " + Utils.murmur2(keyBytes));
            System.out.println("return: " + Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions);
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    @Override
    public void close() {//生命周期方法
        System.out.println("close");
    }

    @Override
    public void configure(Map<String, ?> configs) {//生命周期方法
        System.out.println("configure");
    }
}
