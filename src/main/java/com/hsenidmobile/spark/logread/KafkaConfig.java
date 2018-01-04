package com.hsenidmobile.spark.logread;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.util.*;

/**
 * Created by cloudera on 11/23/17.
 */
public class KafkaConfig {

    Config conf = ConfigFactory.parseResources("TypeSafeConfig.conf");

    public Set<String> getTopicSet() {

        String topics = conf.getString("kafka.topicSet");
        String delimeter = conf.getString("kafka.topicSetSeperator");
        Set<String> topicSet = new HashSet<>(Arrays.asList(topics.split(delimeter)));
        return topicSet;
    }

    public Map<String, Object> getKafkaParams() {
        String brokers = conf.getString("kafka.brokers");
        String bootstrap = conf.getString("kafka.bootstrapServers");
        String groupId = conf.getString("kafka.groupId");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("bootstrap.servers", bootstrap);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        return kafkaParams;
    }


}
