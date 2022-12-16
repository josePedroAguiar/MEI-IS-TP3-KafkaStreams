package pt.uc.dei.streams;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import pt.uc.dei.Serializer.WeatherAlert;
import pt.uc.dei.Serializer.WeatherAlertSerde;
import pt.uc.dei.streams.examplesAndTests.SimpleStreamsExercisesa;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import org.apache.kafka.streams.state.KeyValueStore;

import pt.uc.dei.Serializer.CombinedWeather;
import pt.uc.dei.Serializer.CombinedWeatherSerde;
import pt.uc.dei.Serializer.StandardWeather;
import pt.uc.dei.Serializer.WeatherAlert;
import pt.uc.dei.Serializer.WeatherAlertSerde;

import pt.uc.dei.Serializer.StandardWeatherSerde;



import static java.lang.Math.*;

public class Stream11AverageTemperature {
    public static void main(String[] args) throws InterruptedException, IOException {

                     
                // Create a Kafka Streams configuration object
                Properties props1 = new Properties();
                props1.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application-ff");
                props1.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
                props1.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props1.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, WeatherAlertSerde.class.getName());

                // Create a Kafka Streams configuration object

                // Set up input and output topics
                String inputTopic1 = "weather-alert-33";

                String inputTopic2 = "standard-weather-33";

                String outputFinal = "avg-temp-red-alert-weather-station-3";

                StreamsBuilder builder1 = new StreamsBuilder();
                // StreamsBuilder builder2 = new StreamsBuilder();

                // Read the input topic as a stream of messages
                KStream<String, WeatherAlert> inputStream1 = builder1.stream(inputTopic1,
                                Consumed.with(Serdes.String(), new WeatherAlertSerde()));

                KStream<String, WeatherAlert> filteredStream1 = inputStream1
                                // Filter out events that do not have a red alert
                                .filter((key, value) -> value.getType().equals("red"))
                                .peek((key, value) -> System.out
                                                .println("Stream Join record key " + key + " value "
                                                                + value.getLocation() + " " + value.getType()));

                // Read the input topic as a stream of messages
                KStream<String, StandardWeather> inputStream2 = builder1.stream(inputTopic2,
                                Consumed.with(Serdes.String(), new StandardWeatherSerde()));

                ValueJoiner<StandardWeather, WeatherAlert, CombinedWeather> valueJoiner = (leftValue, rightValue) -> {
                        System.out.println(leftValue.getLocation() + " " + rightValue.getType());
                        CombinedWeather combined = new CombinedWeather(rightValue.getType(), leftValue.getTemperature(),
                                        leftValue.getLocation());
                        return combined;
                };

                Serde weatherAlertSerde = new WeatherAlertSerde();
                Serde standardWeatherSerde = new StandardWeatherSerde();
                Serde combinedWeather = new CombinedWeatherSerde();

                Duration joinWindowSizeMs = Duration.ofHours(1);
                Duration gracePeriod = Duration.ofHours(24);

                KStream<String, CombinedWeather> joinedStream = inputStream2.join(filteredStream1, valueJoiner,
                                JoinWindows.ofTimeDifferenceAndGrace(joinWindowSizeMs, gracePeriod),
                                StreamJoined.with(Serdes.String(), standardWeatherSerde, weatherAlertSerde));

                // joinedStream.groupBy((k, v) -> v.getType()).count();

       
                KGroupedStream<String, CombinedWeather> minTable = joinedStream.groupBy((k, v) -> v.getType(),
                Grouped.with(Serdes.String(), combinedWeather));

                KStream<String, String> finalStream = minTable.aggregate(() -> new int[]{0, 0}, (aggKey, newValue, aggValue) -> {
                        aggValue[0] += 1;
                        aggValue[1] += newValue.getTemperature();
                        return aggValue;
                }, Materialized.with(Serdes.String(), new IntArraySerde()))
                .mapValues(v -> v[0] != 0 ? "" + (1.0 * v[1]) / v[0] : "div by 0").toStream();

                              

                // // in a
                finalStream
                .to(outputFinal, Produced.with(Serdes.String(), Serdes.String()));

                KafkaStreams streams1 = new KafkaStreams(builder1.build(), props1);
                streams1.start();
                // KafkaStreams streams2 = new KafkaStreams(builder2.build(), props2);
                // streams2.start();
                // System.out.println("################################################################3");

        }
}

