package pt.uc.dei.streams;

import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import pt.uc.dei.Serializer.WeatherAlert;
import pt.uc.dei.Serializer.WeatherAlertSerde;
import java.time.Duration;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.*;
import pt.uc.dei.Serializer.CombinedWeather;
import pt.uc.dei.Serializer.CombinedWeatherSerde;
import pt.uc.dei.Serializer.StandardWeather;
import pt.uc.dei.Serializer.StandardWeatherSerde;

public class Stream8MaxTemperatureOfLocation {
        public static void main(String[] args) throws InterruptedException, IOException {

                // Create a Kafka Streams configuration object
                Properties props1 = new Properties();
                props1.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-station-application-8-1");
                props1.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092,broker3:9092");
                props1.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props1.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, WeatherAlertSerde.class.getName());

                // Create a Kafka Streams configuration object
                Properties props2 = new Properties();
                props2.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-station-application-8-2");
                props2.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092,broker3:9092");
                props2.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props2.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StandardWeatherSerde.class.getName());

                Serde<WeatherAlert> weatherAlertSerde = new WeatherAlertSerde();
                Serde<StandardWeather> standardWeatherSerde = new StandardWeatherSerde();
                Serde<CombinedWeather> combinedWeatherSerde = new CombinedWeatherSerde();

                // Set up input and output topics
                String inputTopic1 = "weather-alert";

                String inputTopic2 = "standard-weather";

                String outputFinal = "results";

                StreamsBuilder builder1 = new StreamsBuilder();
                // StreamsBuilder builder2 = new StreamsBuilder();

                // Read the input topic as a stream of messages
                KStream<String, WeatherAlert> inputStream1 = builder1.stream(inputTopic1,
                                Consumed.with(Serdes.String(), new WeatherAlertSerde()));

                // Read the input topic as a stream of messages
                KStream<String, StandardWeather> inputStream2 = builder1.stream(inputTopic2,
                                Consumed.with(Serdes.String(), new StandardWeatherSerde()));

                ValueJoiner<StandardWeather, WeatherAlert, CombinedWeather> valueJoiner = (leftValue, rightValue) -> {
                        System.out.println("Debug1: " + leftValue.getLocation() + " " + rightValue.getType() + " ");
                        CombinedWeather combined = new CombinedWeather(rightValue.getType(), leftValue.getTemperature(),
                                        leftValue.getLocation());
                        return combined;
                };

                Duration joinWindowSizeMs = Duration.ofHours(1);
                Duration gracePeriod = Duration.ofHours(24);

                KStream<String, CombinedWeather> joinedStream = inputStream2.join(inputStream1, valueJoiner,
                                JoinWindows.ofTimeDifferenceAndGrace(joinWindowSizeMs, gracePeriod),
                                StreamJoined.with(Serdes.String(), standardWeatherSerde, weatherAlertSerde));

                Duration windowSize = Duration.ofMinutes(60);
                TimeWindows window = TimeWindows.ofSizeAndGrace(windowSize, gracePeriod);

                KGroupedStream<String, CombinedWeather> minTable = joinedStream
                                .groupBy((k, v) -> v.getLocation(),
                                                Grouped.with(Serdes.String(), combinedWeatherSerde));

                KStream<String, String> finalStream = minTable.windowedBy(window).aggregate(
                                () -> Integer.MIN_VALUE, // Initialize the aggregation value with the
                                                         // maximum possible integer
                                                         // value
                                (key, value, aggregate) -> Math.max(value.getTemperature(), aggregate), // Use
                                                                                                        // the
                                                                                                        // min
                                                                                                        // function
                                                                                                        // to
                                                                                                        // calculate
                                                                                                        // the
                                                                                                        // minimum
                                                                                                        // value
                                Materialized.with(Serdes.String(), Serdes.Integer()))
                                .mapValues(v -> Integer.toString(v))
                                .toStream((wk, v) -> wk.key()).peek((k, v) -> System.out.println("Value: " + v));

                finalStream.to(outputFinal, Produced.with(Serdes.String(), Serdes.String()));

                KafkaStreams streams1 = new KafkaStreams(builder1.build(), props1);
                streams1.start();

        }
}
