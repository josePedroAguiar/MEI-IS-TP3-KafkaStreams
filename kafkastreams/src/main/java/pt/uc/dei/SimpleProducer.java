package pt.uc.dei;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import pt.uc.dei.Serializer.StandardWeather;
import pt.uc.dei.Serializer.StandardWeatherSerde;
import pt.uc.dei.Serializer.WeatherAlert;

public class SimpleProducer {

    public static void main(String[] args) throws Exception { // Assign topicName to string variable

        String topicName = args[0].toString();

        // create instance for properties to access producer configs
        Properties props = new Properties(); // Assign localhost id
        
        props.put("bootstrap.servers", "broker1:9092");
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
        props.put("value.serializer", StandardWeatherSerde.class.getName());

        /*
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
        */
        Random r = new Random();
        ObjectMapper mapper = new ObjectMapper();

        if (topicName.equals("standard-weather7")) {
            Producer<String, StandardWeather> producer = new KafkaProducer<>(props);

            for (int i = 0; i < 10; i++) {

                String[] a = { "Distrito de Beja", "Distrito de Évora", "Distrito de Santarém",
                        "Distrito de Castelo Branco", "Distrito de Bragança", "Distrito de Portalegre",
                        "Distrito da Guarda", "Distrito de Setúbal", "Distrito de Viseu", "Distrito de Faro",
                        "Distrito de Vila Real", "Distrito de Coimbra", "Distrito de Leiria", "Distrito de Aveiro",
                        "Distrito de Lisboa", "Distrito de Braga", "Distrito do Porto", "Região Autónoma dos Açores",
                        "Distrito de Viana do Castelo", "Região Autónoma da Madeira" };
                List<String> list = Arrays.asList(a);
                System.out.println(list.size());
                //JSONObject user = new JSONObject();

                int pos = r.nextInt(list.size());
                String[] condition = { "fog", "rain", "snow", "hail" };
                List<String> list1 = Arrays.asList(condition);
                int pos1 = r.nextInt(list1.size());

                StandardWeather user = new StandardWeather(r.nextInt(50) - 10, list.get(pos));
                //JsonNode node = mapper.valueToTree(user);
                //user.put("atmospheric condition",list1.get(pos1));
                //user.put("temperature", r.nextInt(40));
                //user.put("location",list.get(pos) );

                producer.send(
                        new ProducerRecord<String,StandardWeather>(topicName, Integer.toString((pos % 4)), user));
                if (i % 100 == 0)
                    System.out.println("Sending message " + (i + 1) + " to topic " + topicName);
            }
            producer.close();

        }
        else if (topicName.equals("weather-alert5")) {
            //Producer<String, String> producer = new KafkaProducer<>(props);
            Producer<String, String> producer = new KafkaProducer<>(props);

            for (int i = 0; i < 10; i++) {

                String[] a = { "Distrito de Beja", "Distrito de Évora", "Distrito de Santarém",
                        "Distrito de Castelo Branco", "Distrito de Bragança", "Distrito de Portalegre",
                        "Distrito da Guarda", "Distrito de Setúbal", "Distrito de Viseu", "Distrito de Faro",
                        "Distrito de Vila Real", "Distrito de Coimbra", "Distrito de Leiria", "Distrito de Aveiro",
                        "Distrito de Lisboa", "Distrito de Braga", "Distrito do Porto", "Região Autónoma dos Açores",
                        "Distrito de Viana do Castelo", "Região Autónoma da Madeira" };
                List<String> list = Arrays.asList(a);
                System.out.println(list.size());
                JSONObject user = new JSONObject();
                int pos = r.nextInt(list.size());

                String[] flag = { "red", "green" };
                int f = r.nextInt(2);
                String[] condition = { "hunderstorms", "hurricanes", "blizzards", "droughts" };
                List<String> list1 = Arrays.asList(condition);
                int pos1 = r.nextInt(list1.size());

                user.put("atmospheric condition",list1.get(pos1));
                user.put("flag",flag[f] );
                user.put("location",list.get(pos) );

                //WeatherAlert user = new WeatherAlert(flag[f], list.get(pos));
                //JsonNode node = mapper.valueToTree(user);


                producer.send(new ProducerRecord<String, String>(topicName, Integer.toString((pos % 4)), user.toString()));
                if (i % 100 == 0)
                    System.out.println("Sending message " + (i + 1) + " to topic " + topicName);

            }
            producer.close();

        }
    }
}