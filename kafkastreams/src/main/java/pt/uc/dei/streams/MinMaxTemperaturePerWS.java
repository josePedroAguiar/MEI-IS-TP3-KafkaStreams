package pt.uc.dei.streams;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import pt.uc.dei.Serializer.DoublePair;
import pt.uc.dei.Serializer.StandardWeather;
import pt.uc.dei.Serializer.StandardWeatherSerde;
import static java.lang.Math.*;

public class MinMaxTemperaturePerWS {
    public static void main(String[] args) throws InterruptedException, IOException {
        StandardWeatherSerde standardWeatherSerde;
        if (args.length != 2) {
            System.err.println("Wrong arguments. Please run the class as follows:");
            System.err.println(SimpleStreamsExercisesa.class.getName() + " input-topic output-topic");
            System.exit(1);
        }
        // Create a Kafka Streams configuration object
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application-a");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StandardWeatherSerde.class.getName());

        // Define the input and output topics
        String inputTopic = "standard-weather10";
        String outputTopic = "temperature-readings-count2";

        // Create a Kafka Streams builder
        StreamsBuilder builder = new StreamsBuilder();

        // create a stream of weather data
        KStream<String, StandardWeather> weatherStream = builder.stream("weather-data");

        // group the stream by location
        KGroupedStream<String, StandardWeather> groupedWeatherStream = weatherStream
                .groupByKey();

        /*KTable<String, DoublePair> minMaxTemperatureTable = groupedWeatherStream.aggregate(
            // initializer for the aggregation state
            () -> new DoublePair(-30, 100),
            // function to add a new value to the aggregation state
            (key, value, aggregate) -> new DoublePair(
                value.getTemperature() < aggregate.getFirst() ? value.getTemperature() : aggregate.getFirst(),
                value.getTemperature() > aggregate.getSecond() ? value.getTemperature() : aggregate.getSecond()),
            // function to combine two aggregation states
            (aggregate1, aggregate2) -> new DoublePair(
                aggregate1.getFirst() < aggregate2.getFirst() ? aggregate1.getFirst() : aggregate2.getFirst(),
                aggregate1.getSecond() > aggregate2.getSecond() ? aggregate1.getSecond() : aggregate2.getSecond()),
            Materialized.as("min-max-temperature-per-location")
        );
        */
    }
}