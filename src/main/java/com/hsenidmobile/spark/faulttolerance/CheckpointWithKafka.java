package com.hsenidmobile.spark.faulttolerance; /**
 * Created by cloudera on 10/30/17.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static jodd.util.ThreadUtil.sleep;

public class CheckpointWithKafka {

    private static long global_count = 0;
    //private static final Pattern SPACE = Pattern.compile(" ");
    private static final String CHECKPOINT_DIRECTORY = "/home/cloudera/Documents/streaming_dir/checkpoint_dir";
    //private static final String OUTPUT_PATH = "/home/cloudera/Documents/streaming_dir/output_dir";

    public static void main(String[] args) throws Exception {


        String brokers = "localhost:9092";
        String topics = "kafka_topic_checkpoint";
        JavaStreamingContext ssc;

        Function0<JavaStreamingContext> createContextFunc =
                () -> getCount(CHECKPOINT_DIRECTORY, brokers, topics);

        ssc = JavaStreamingContext.getOrCreate(CHECKPOINT_DIRECTORY, createContextFunc);
        ssc.start();
        ssc.awaitTerminationOrTimeout(5000);
        ssc.stop();
        ssc = JavaStreamingContext.getOrCreate(CHECKPOINT_DIRECTORY, (Function0<JavaStreamingContext>) () -> {
            throw new IllegalStateException("Context should be created from checkpoint");
        });
        ssc.start();

    }

    public static JavaStreamingContext getCount(String checkpointDirectory, String brokers, String topics) throws Exception {


        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        SparkConf conf = new SparkConf().setAppName("StreamGroupFunction").setMaster("local[2]")
                .set("spark.streaming.receiver.writeAheadLog.enable", "true");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(2000));
        ssc.checkpoint(checkpointDirectory);


        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));


        // Get the lines, split them into words, count the words and print
        JavaDStream<String> logdata = messages.map(ConsumerRecord::value);

        logdata.print();

        JavaDStream<String> tranStream = logdata
//                .persist(StorageLevel.DISK_ONLY_2())
                .map(line -> {

                    return line;
                });

        tranStream.print();





        tranStream.foreachRDD(rdd -> {

            System.out.println("count : " + rdd.count());
            System.out.println("global count :" + global_count);

            writeFile(rdd.count());

        });

        return ssc;

    }

    public static void writeFile(long count) {

        try {
            global_count = global_count + count;

            String content = String.valueOf(global_count);

            File file = new File("/home/cloudera/Downloads/counterFile");

            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fileWriter = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(content);
            bufferedWriter.close();

            sleep(5000);

            System.out.println("Done");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}