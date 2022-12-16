package pt.uc.dei.streams;

import java.io.IOException;
import java.io.Serializable;
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

public class Stream8MaxTemperatureOfLocation {
        public static void main(String[] args) throws InterruptedException, IOException {

                // if (args.length != 2) {
                //         System.err.println("Wrong arguments. Please run the class as follows:");
                //         System.err.println(SimpleStreamsExercisesa.class.getName() + " input-topic output-topic");
                //         System.exit(1);
                // }
                // Create a Kafka Streams configuration object
                Properties props1 = new Properties();
                props1.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application-a");
                props1.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092,broker3:9092");
                props1.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props1.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, WeatherAlertSerde.class.getName());

                // Create a Kafka Streams configuration object
                Properties props2 = new Properties();
                props2.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application-b");
                props2.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092,broker3:9092");
                props2.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props2.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StandardWeatherSerde.class.getName());

                // Set up input and output topics
                String inputTopic1 = "weather-alert-1122";

                String inputTopic2 = "standard-weather-1122";

                String outputFinal = "max-temp-red-alert-weather-station-1122";

                StreamsBuilder builder1 = new StreamsBuilder();
                // StreamsBuilder builder2 = new StreamsBuilder();

                // Read the input topic as a stream of messages
                KStream<String, WeatherAlert> inputStream1 = builder1.stream(inputTopic1,
                                Consumed.with(Serdes.String(), new WeatherAlertSerde()))
                                ;
                        
                // Read the input topic as a stream of messages
                KStream<String, StandardWeather> inputStream2 = builder1.stream(inputTopic2,
                                Consumed.with(Serdes.String(), new StandardWeatherSerde())
                                );

                ValueJoiner<StandardWeather, WeatherAlert, CombinedWeather> valueJoiner = (leftValue, rightValue) -> {
                        System.out.println("Debug1: "+leftValue.getLocation() + " " + rightValue.getType() + " ");
                        CombinedWeather combined = new CombinedWeather(rightValue.getType(), leftValue.getTemperature(),
                                        leftValue.getLocation());
                        return combined;
                };

                Serde weatherAlertSerde = new WeatherAlertSerde();
                Serde standardWeatherSerde = new StandardWeatherSerde();
                Serde combinedWeather = new CombinedWeatherSerde();

                Duration joinWindowSizeMs = Duration.ofHours(1);
                Duration gracePeriod = Duration.ofHours(24);

                KStream<String, CombinedWeather> joinedStream = inputStream2.join(inputStream1, valueJoiner,
                                JoinWindows.ofTimeDifferenceAndGrace(joinWindowSizeMs, gracePeriod),
                                StreamJoined.with(Serdes.String(), standardWeatherSerde, weatherAlertSerde));

                // Duration windowSize = Duration.ofMinutes(5);
                // Duration advanceSize = Duration.ofMinutes(1);
                // TimeWindows hoppingWindow =
                // TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advanceSize);
                Duration windowSize = Duration.ofMinutes(1440);
                TimeWindows window = TimeWindows.of(windowSize);

                // joinedStream.groupBy((k, v) -> v.getType()).count();

                // testar->|
                // |
                // v
                KGroupedStream<String, CombinedWeather> minTable = joinedStream
                                .groupBy((k, v) -> v.getLocation(),
                                                Grouped.with(Serdes.String(), combinedWeather));
                
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
                                .toStream((wk, v) -> wk.key()).peek((k, v) -> System.out.println("Valueeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee: " + v));
                
                /*KStream<String, String> finalStream = minTable.aggregate(
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
                                                                                                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(
                                                                                                    "max-store-location").withValueSerde(Serdes.Integer())
                )
                .mapValues(v -> Integer.toString(v))
                .toStream().peek((k, v) -> System.out.println("Value: "+v));
                */
                // // in a
                finalStream.to(outputFinal, Produced.with(Serdes.String(), Serdes.String()));

                KafkaStreams streams1 = new KafkaStreams(builder1.build(), props1);
                streams1.start();
                // KafkaStreams streams2 = new KafkaStreams(builder2.build(), props2);
                // streams2.start();
                // System.out.println("################################################################3");

        }
}
