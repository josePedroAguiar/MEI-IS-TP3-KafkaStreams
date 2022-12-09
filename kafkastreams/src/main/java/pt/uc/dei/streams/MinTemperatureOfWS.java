package pt.uc.dei.streams;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import pt.uc.dei.Serializer.StandardWeather;
import pt.uc.dei.Serializer.WeatherAlert;
import pt.uc.dei.Serializer.WeatherAlertSerde;

import pt.uc.dei.Serializer.StandardWeatherSerde;

public class MinTemperatureOfWS {
    public static void main(String[] args) throws InterruptedException, IOException {

        if (args.length != 2) {
            System.err.println("Wrong arguments. Please run the class as follows:");
            System.err.println(SimpleStreamsExercisesa.class.getName() + " input-topic output-topic");
            System.exit(1);
        }
        // Create a Kafka Streams configuration object
        Properties props1 = new Properties();
        props1.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application-a");
        props1.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        props1.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props1.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, WeatherAlertSerde.class.getName());
        StreamsBuilder builder1 = new StreamsBuilder();

        KafkaStreams streams1 = new KafkaStreams(builder1.build(), props1);

        // Create a Kafka Streams configuration object
        Properties props2 = new Properties();
        props2.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application-b");
        props2.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        props2.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props2.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StandardWeatherSerde.class.getName());
        StreamsBuilder builder2 = new StreamsBuilder();

        KafkaStreams streams2 = new KafkaStreams(builder2.build(), props2);

        // Set up input and output topics
        String inputTopic1 = "weather-alert11";

        String inputTopic2 = "standard-weather11";

        String outputFinal = "min-temp-red-alert-weather-station";

        // Read the input topic as a stream of messages
        KStream<String, WeatherAlert> inputStream1 = builder1.stream(inputTopic1,
                Consumed.with(Serdes.String(), new WeatherAlertSerde()));

        KTable<String, String> filteredStream1 = inputStream1
                // Join the input stream with the output from the first Kafka Streams instance
                .join(filteredStream1, (weather, alert) -> weather)
                // Filter out events that do not have a red alert
                .filter((key, value) -> value.equals("red"))
                .groupByKey();
        // Read the input topic as a stream of messages
        KStream<String, WeatherAlert> inputStream2 = builder2.stream(inputTopic2,
                Consumed.with(Serdes.String(), new WeatherAlertSerde()));

        Duration joinWindowSizeMs = Duration.ofHours(1);
        Duration gracePeriod = Duration.ofHours(24);
        KTable<String, String> filteredStream2 = inputStream2
                // Join the events from the "standard-weather" and "red-alert-weather-station"
                // topics based on their weather station name
                .join(filteredStream1,
                        (standardWeather, alert) -> standardWeather,
                        JoinWindows.ofTimeDifferenceAndGrace(joinWindowSizeMs,gracePeriod ))
                // Calculate the minimum temperature of the weather stations with red alert
                // events
                .groupBy((key, value) -> "min-temp-red-alert-weather-station",
                        Grouped.with(Serdes.String(), new StandardWeatherSerde()))
                .reduce((aggValue, newValue) -> newValue.getTemperature() < aggValue.getTemperature() ? newValue
                        : aggValue);

        // Write the output to the specified output topic
        filteredStream2.toStream().to(outputFinal, Produced.with(Serdes.String(), new StandardWeatherSerde()));

        // Start processing the input streams
        streams1.start();
        streams2.start();
    }
}
