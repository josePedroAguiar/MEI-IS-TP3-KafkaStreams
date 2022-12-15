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

import pt.uc.dei.Serializer.WeatherAlert;
import pt.uc.dei.Serializer.WeatherAlertSerde;
import pt.uc.dei.streams.examplesAndTests.SimpleStreamsExercisesa;

public class Stream6AlertsPerType {
    public static void main(String[] args) throws InterruptedException, IOException {

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
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, WeatherAlertSerde.class.getName());

        // Define the input and output topics
        String inputTopic = "weather-alert11";
        String outputTopic = "temperature-readings-count3";

        // Create a Kafka Streams builder
        StreamsBuilder builder = new StreamsBuilder();

        // Read the input topic as a stream of messages
        KStream<String, WeatherAlert> inputStream = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), new WeatherAlertSerde()));

        // Extract the temperature and weather station data from the messages
        // KStream<String, WeatherAlert> temperatureReadingsStream =
        // inputStream.map((key, value) -> new KeyValue<String, WeatherAlert> (key,
        // new WeatherAlert(value.getTemperature(), value.getLocation())));

        // Group the data by weather station
        KGroupedStream<String, WeatherAlert> temperatureReadingsGroupedByWeatherStation = inputStream
            .groupBy((key, value) -> value.getType());

        // Count the number of temperature readings per weather station
        KTable<String, Long> temperatureReadingsCount = temperatureReadingsGroupedByWeatherStation.count();

        // Write the result to the output topic
        temperatureReadingsCount.toStream().mapValues(v->Long.toString(v)).to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        // Create the Kafka Streams instance
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);

        // Start the Kafka Streams instance
        streams.start();
    }
}
