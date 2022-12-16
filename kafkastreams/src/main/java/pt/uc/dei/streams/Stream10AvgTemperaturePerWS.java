package pt.uc.dei.streams;

import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import pt.uc.dei.Serializer.IntArraySerde;
import pt.uc.dei.Serializer.StandardWeather;
import pt.uc.dei.Serializer.StandardWeatherSerde;


public class Stream10AvgTemperaturePerWS {
        public static void main(String[] args) throws InterruptedException, IOException {
                // Create a Kafka Streams configuration object
                Properties streamsConfig = new Properties();
                streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-station-application-10");
                streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092,broker3:9092");
                streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StandardWeatherSerde.class);

                // Define the input and output topics
                String inputTopic = "standard-weather";
                String outputTopic = "results";

                // Create a Kafka Streams builder
                StreamsBuilder builder = new StreamsBuilder();

                // create a stream of weather data
                KStream<String, StandardWeather> weatherStream = builder.stream(inputTopic);

                // group the stream by location
                weatherStream.groupByKey()
                                .aggregate(() -> new int[] { 0, 0 }, (aggKey, newValue, aggValue) -> {
                                        aggValue[0] += 1;
                                        aggValue[1] += newValue.getTemperature();
                                        return aggValue;
                                }, Materialized.with(Serdes.String(), new IntArraySerde()))
                                .mapValues(v -> v[0] != 0 ? "" + (1.0 * v[1]) / v[0] : "div by 0")
                                .toStream()
                                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

                // Create the Kafka Streams instance
                KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);

                // Start the Kafka Streams instance
                streams.start();
        }
}