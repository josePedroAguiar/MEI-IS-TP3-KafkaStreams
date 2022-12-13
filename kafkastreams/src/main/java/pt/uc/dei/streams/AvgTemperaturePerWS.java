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

public class AvgTemperaturePerWS {
        public static void main(String[] args) throws InterruptedException, IOException {
                StandardWeatherSerde standardWeatherSerde;
                if (args.length != 2) {
                        System.err.println("Wrong arguments. Please run the class as follows:");
                        System.err.println(SimpleStreamsExercisesa.class.getName() + " input-topic output-topic");
                        System.exit(1);
                }
                // Create a Kafka Streams configuration object
                Properties streamsConfig = new Properties();
                streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application-111");
                streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
                streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StandardWeatherSerde.class);

                // Define the input and output topics
                String inputTopic = "standard-weather34";
                String outputTopic = "temperature-readings-count34";

                // Create a Kafka Streams builder
                StreamsBuilder builder = new StreamsBuilder();
                final Serde<DoublePair> serde = new DoublePairSerde();


                // create a stream of weather data
                KStream<String, StandardWeather> weatherStream = builder.stream(inputTopic);

                // group the stream by location

                 weatherStream.groupByKey()
                                .aggregate(() -> new int[]{0, 0}, (aggKey, newValue, aggValue) -> {
                                        aggValue[0] += 1;
                                        aggValue[1] += newValue.getTemperature();
                                        return aggValue;
                                }, Materialized.with(Serdes.String(), new IntArraySerde()))
                        .mapValues(v -> v[0] != 0 ? "" + (1.0 * v[1]) / v[0] : "div by 0")
            .toStream()
            .to( outputTopic, Produced.with(Serdes.String(), Serdes.String()));

              

                // Write the result to the output topic
                // minTable.toStream().to(outputTopic, Produced.with(Serdes.String(),
                // Serdes.Integer()));

                // Create the Kafka Streams instance
                KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);

                // Start the Kafka Streams instance
                streams.start();

        }

}