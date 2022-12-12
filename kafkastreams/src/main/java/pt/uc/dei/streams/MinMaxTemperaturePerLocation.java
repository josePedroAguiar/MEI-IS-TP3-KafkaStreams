package pt.uc.dei.streams;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
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
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.state.KeyValueStore;

import pt.uc.dei.Serializer.DoublePair;
import pt.uc.dei.Serializer.DoublePairSerde;
import pt.uc.dei.Serializer.StandardWeather;
import pt.uc.dei.Serializer.StandardWeatherSerde;
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
                streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StandardWeatherSerde.class);

                // Define the input and output topics
                String inputTopic = "standard-weather23";
                String outputTopic = "temperature-readings-count23";

                // Create a Kafka Streams builder
                StreamsBuilder builder = new StreamsBuilder();
                final Serde<DoublePair> serde = new DoublePairSerde();


                // create a stream of weather data
                KStream<String, StandardWeather> weatherStream = builder.stream(inputTopic);

                // group the stream by location

                KTable<String, Integer> maxTable = weatherStream
                                .groupBy((key, value) -> value.getLocation())
                                .aggregate(
                                                () -> Integer.MIN_VALUE, // Initialize the aggregation value with the
                                                                         // minimum possible integer
                                                                         // value
                                                (key, value, aggregate) -> Math.max(value.getTemperature(), aggregate), // Use
                                                                                                                        // the
                                                                                                                        // max
                                                                                                                        // function
                                                                                                                        // to
                                                                                                                        // calculate
                                                                                                                        // the
                                                                                                                        // maximum
                                                                                                                        // value
                                                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(
                                                                "max-store").withValueSerde(Serdes.Integer())// Materialize
                                                                                                             // the
                                                                                                             // aggregation
                                                                                                             // results
                                                                                                             // in a
                                                                                                             // store
                                                                                                             // named
                                                                                                             // "max-store"
                                );

                // Use the min aggregation function to calculate the minimum value in the stream
                KTable<String, Integer> minTable = weatherStream
                                .groupBy((key, value) -> value.getLocation())
                                .aggregate(
                                                () -> Integer.MAX_VALUE, // Initialize the aggregation value with the
                                                                         // maximum possible integer
                                                                         // value
                                                (key, value, aggregate) -> Math.min(value.getTemperature(), aggregate), // Use
                                                                                                                        // the
                                                                                                                        // min
                                                                                                                        // function
                                                                                                                        // to
                                                                                                                        // calculate
                                                                                                                        // the
                                                                                                                        // minimum
                                                                                                                        // value
                                                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(
                                                                "min-store").withValueSerde(Serdes.Integer()) // Materialize
                                                                                                              // the
                                                                                                              // aggregation
                                                                                                              // results
                                                                                                              // in a
                                // store named "min-store"
                                );

                KStream<String, DoublePair> outputStream = maxTable
                                .join(minTable, (max, min) -> new DoublePair(max, min)).toStream();

                outputStream.to(outputTopic, Produced.with(Serdes.String(), serde));
                // Write the result to the output topic
                // minTable.toStream().to(outputTopic, Produced.with(Serdes.String(),
                // Serdes.Integer()));

                // Create the Kafka Streams instance
                KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);

                // Start the Kafka Streams instance
                streams.start();

        }

}