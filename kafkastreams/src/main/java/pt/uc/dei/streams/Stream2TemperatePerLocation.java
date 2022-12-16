package pt.uc.dei.streams;

import java.io.IOException;
import java.util.Properties;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import pt.uc.dei.Serializer.StandardWeather;
import pt.uc.dei.Serializer.StandardWeatherSerde;

public class Stream2TemperatePerLocation {
    public static void main(String[] args) throws InterruptedException, IOException {
        // Create a Kafka Streams configuration object
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-station-application-2");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092,broker3:9092");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StandardWeatherSerde.class.getName());


        // Define the input and output topics
        String inputTopic = "standard-weather";
        String outputTopic = "results";

        // Create a Kafka Streams builder
        StreamsBuilder builder = new StreamsBuilder();

        // Read the input topic as a stream of messages
        KStream<String, StandardWeather> inputStream = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), new StandardWeatherSerde()));
                
        // Group the data by Location
        KGroupedStream<String, StandardWeather> temperatureReadingsGroupedByLocation = inputStream
                .groupBy((key, value) -> value.getLocation());

        // Count the number of temperature readings per Location station
        KTable<String, Long> temperatureReadingsCount = temperatureReadingsGroupedByLocation.count();

        // Write the result to the output topic
        temperatureReadingsCount.toStream().mapValues(v->Long.toString(v)).to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        // Create the Kafka Streams instance
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);

        // Start the Kafka Streams instance
        streams.start();
    }
}