package com.hsenidmobile.spark.faulttolerance;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.joda.time.DateTime;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by cloudera on 11/22/17.
 */
public class SparkReliabilityTest {

    private static long global_count = 0;
    //private static final Pattern SPACE = Pattern.compile(" ");
    private static final String CHECKPOINT_DIRECTORY = "/home/hsenid/Documents/streaming_data_checkpoint";
    private static Logger logger = Logger.getLogger(SparkReliabilityTest.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: Checkpoint <hostname> <port>");
            System.exit(1);
        }

        String brokers = "localhost:9092";
        String topics = args[0];
        JavaStreamingContext ssc;

        Function0<JavaStreamingContext> createContextFunc = () -> getCount(brokers, topics);

        ssc = JavaStreamingContext.getOrCreate(CHECKPOINT_DIRECTORY, createContextFunc);
        ssc.start();

        logger.warn("Started in this time : " + DateTime.now());
        ssc.awaitTermination();

        /*ssc.stop();
        logger.warn("Stopped and restarting : " + DateTime.now());
        writeFile("Stopped at this point " + DateTime.now());
        ssc = JavaStreamingContext.getOrCreate(CHECKPOINT_DIRECTORY, (Function0<JavaStreamingContext>) () -> {
            throw new IllegalStateException("Context should be created from checkpoint");
        });
        ssc.start();
        logger.warn("Restarted " );
        ssc.awaitTermination();*/

    }

    public static JavaStreamingContext getCount(String brokers, String topics) throws Exception {


        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        SparkConf conf = new SparkConf().setAppName("StreamGroupFunction").setMaster("local[2]");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(10000));
        ssc.checkpoint(CHECKPOINT_DIRECTORY);


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
                    writeFile(line);
                    logger.info(DateTime.now() + "   " + line);
                    return line;
                });

        tranStream.print();

        tranStream.foreachRDD(rdd -> {

//            System.out.println("count : " + rdd.count());
//            System.out.println("global count :" + global_count);

        });

        return ssc;

    }

    public static void writeFile(String lines) {

        try {

            File file = new File("/home/hsenid/Documents/output_from_intellij.txt");

            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fileWriter = new FileWriter(file.getAbsoluteFile(), true);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(lines);
            bufferedWriter.close();


            System.out.println("Done");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
