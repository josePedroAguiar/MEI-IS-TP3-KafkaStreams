package pt.uc.dei;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;


import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import pt.uc.dei.Serializer.StandardWeather;
import pt.uc.dei.Serializer.StandardWeatherSerde;
import pt.uc.dei.Serializer.WeatherAlert;
import pt.uc.dei.Serializer.WeatherAlertSerde;


public class SimpleProducer {

    static final String[] topics = { "standard-weather-33", "weather-alert-33" };
    static final List<String> topicNames = Arrays.asList(topics);

    static final String[] al = { "Distrito de Beja", "Distrito de Évora", "Distrito de Santarém",
            "Distrito de Castelo Branco", "Distrito de Bragança", "Distrito de Portalegre",
            "Distrito da Guarda", "Distrito de Setúbal", "Distrito de Viseu", "Distrito de Faro",
            "Distrito de Vila Real", "Distrito de Coimbra", "Distrito de Leiria", "Distrito de Aveiro",
            "Distrito de Lisboa", "Distrito de Braga", "Distrito do Porto", "Região Autónoma dos Açores",
            "Distrito de Viana do Castelo", "Região Autónoma da Madeira" };

    static final String[] flag = { "red", "green" };

    static Random r = new Random();
    static WriteFiles filewriter=new WriteFiles();

    public static void main(String[] args) throws Exception { // Assign topicName to string variable
        String topicName;
        Properties props = new Properties(); // Assign localhost id

        props.put("bootstrap.servers", "broker1:9092,broker2:9092,broker3:9092");
        // Set acknowledgements for producer requests. props.put("acks", "all");
        // If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        // Specify buffer size in config
        props.put("batch.size", 16384);
        // Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        // The buffer.memory controls the total amount of memory available to the
        // producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if (args.length == 1) {
            System.out.println(
                    "-------------------------------------------------------\n\t\t:)\n------------------------------------------------------");
            String topic = args[0].toString();
            createContentTopic(props, topic);
        }

        else if (args.length == 2) {
            System.out.println(
                    "-------------------------------------------------------\n\t\t:(\n------------------------------------------------------");
            String topic = args[0].toString();

            if (topic.equals("standard")) {
                topicName = args[1].toString();
                ;
                createStandardWeather(topicName, props);

            } else if (topic.equals("alert")) {
                topicName = args[1].toString();
                ;
                createAlertEvent(topicName, props);

            } else {
                System.out.println("ERRO: First Agument is invalid: try run:\n" +
                        "/usr/bin/env /usr/java/openjdk-18/bin/java @/tmp/cp_56nelkrk30b1jpsm5hfdywsn.argfile pt.uc.dei.SimpleProducer %{alert,standard} %topicName");
                return;
            }
        }

        else {
            System.out.println(
                    "-------------------------------------------------------\n\t\t-_-\n------------------------------------------------------");
            for (String topic : topicNames) {
                topicName = topic;
                if (topicName.equals(topics[0])) {
                    createStandardWeather(topicName, props);
                } else {
                    createAlertEvent(topicName, props);
                }

            }
        }
    }

    static void createStandardWeather(String topicName, Properties props) {
        props.put("value.serializer", StandardWeatherSerde.class.getName());

        Producer<String, StandardWeather> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            int key = r.nextInt(al.length * 4);
            int pos = key / 4;
            StandardWeather user = new StandardWeather(1, al[pos]);
            String out = "Key: " + Integer.toString(key) + " Temperature: " + 1
                    + " Location: " + user.getLocation()+"\n";
            System.out.println(out);
            WriteFiles.writeToFile("Producer_" + topicName + "s.txt", out);
            producer.send(
                    new ProducerRecord<String, StandardWeather>(topicName, Integer.toString(key), user));

        }
        producer.close();
    }

    static void createAlertEvent(String topicName, Properties props) {
        props.put("value.serializer", WeatherAlertSerde.class.getName());
        Producer<String, WeatherAlert> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            int key = r.nextInt(al.length * 4);
            int pos = key / 4;
            WeatherAlert user = new WeatherAlert(flag[r.nextInt(2)], al[pos]);
            String out = "Key: " + Integer.toString(key) + " type: " + user.getType() + " Location: "
                    + user.getLocation()+"\n";
            System.out.println(out);
            WriteFiles.writeToFile("Producer_" + topicName + "s.txt", out);
            producer.send(new ProducerRecord<String, WeatherAlert>(topicName, Integer.toString(key), user));
        }
        producer.close();
    }

    static public void createContentTopic(Properties props, String topic) {
        if (topic.equals("standard")) {
            String topicName = topics[0];
            createStandardWeather(topicName, props);

        } else if (topic.equals("alert")) {
            String topicName = topics[1];
            createAlertEvent(topicName, props);

        }
    }

}