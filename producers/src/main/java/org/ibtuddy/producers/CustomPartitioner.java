package org.ibtuddy.producers;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class CustomPartitioner implements Partitioner {

    private String specialKey;

    @Override
    public void configure(Map<String, ?> configs) {
        specialKey = configs.get("custom.specialKey").toString() ;
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
        Cluster cluster) {
        List<PartitionInfo> partitionInfoList =cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfoList.size();
        int numSpecialPartitions = (int)(numPartitions * 0.5);
        int partitionIndex = 0;

        if(((String)key).equals("P001")){
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions;
        }else{
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % (numPartitions - numSpecialPartitions) + numSpecialPartitions;
        }

        return partitionIndex;

    }

    @Override
    public void close() {

    }


}
