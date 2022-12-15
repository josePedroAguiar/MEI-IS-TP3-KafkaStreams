package pt.uc.dei;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import pt.uc.dei.Serializer.DoublePair;
import pt.uc.dei.Serializer.DoublePairSerde;
import pt.uc.dei.Serializer.StandardWeather;
import pt.uc.dei.Serializer.StandardWeatherSerde;

public class SimpleConsumer {
    public static void main(String[] args) throws Exception{
        //Assign topicName to string variable
        String topicName = args[0].toString();
        // create instance for properties to access producer configs
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", "broker1:9092"); //Set acknowledgements for producer requests. props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("value.deserializer", StandardWeatherSerde.class.getName());
        //Consumer<String, StandardWeather> consumer = new KafkaConsumer<>(props); consumer.subscribe(Collections.singletonList(topicName));
        
        //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        //Consumer<String, Long> consumer = new KafkaConsumer<>(props); consumer.subscribe(Collections.singletonList(topicName));


        //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("value.deserializer", DoublePairSerde.class.getName());
        //Consumer<String, DoublePair> consumer = new KafkaConsumer<>(props); consumer.subscribe(Collections.singletonList(topicName));

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String, String> consumer = new KafkaConsumer<>(props); consumer.subscribe(Collections.singletonList(topicName));
             /*try {
            while (true) {
                Duration d = Duration.ofSeconds(1000000);
                ConsumerRecords<String, StandardWeather> records = consumer.poll(d);
                for (ConsumerRecord<String, StandardWeather> record : records) {
                    System.out.println(record.key() + " => " + record.value().getLocation()); 
                }
                return;
            }    
        }
        finally {
            consumer.close();
        }*/
        /*try {
            while (true) {
                Duration d = Duration.ofSeconds(1000000);
                ConsumerRecords<String, StandardWeather> records = consumer.poll(d);
                for (ConsumerRecord<String, StandardWeather> record : records) {
                    System.out.println(record.key() + " => " + record.value().getLocation()); 
                }
                return;
            }    
        }
        finally {
            consumer.close();
        }*/

        /*try {
            while (true) {
                Duration d = Duration.ofSeconds(1000000);
                ConsumerRecords<String, DoublePair> records = consumer.poll(d);
                for (ConsumerRecord<String, DoublePair> record : records) {
                    System.out.println(record.key() + " => " + record.value().getMax() + " "+ record.value().getMin()); 
                }
                return;
            }    
        }
        finally {
            consumer.close();
        }*/
        try {
            while (true) {
                Duration d = Duration.ofSeconds(1000000);
                ConsumerRecords<String, String> records = consumer.poll(d);
                for (ConsumerRecord<String, String> record : records) {
                    String out= record.key() + " => " + record.value()+"\n";
                    System.out.println(out);
                    WriteFiles.writeToFile("Consumer_" + topicName + "s.txt", out); 
                }
                return;
            }    
        }
        finally {
            consumer.close();
        }
    } 
}