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
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;

import pt.uc.dei.Serializer.CombinedWeather;
import pt.uc.dei.Serializer.CombinedWeatherSerde;
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
        props1.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application-b");
        props1.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        props1.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props1.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, WeatherAlertSerde.class.getName());

        // Create a Kafka Streams configuration object
        Properties props2 = new Properties();
        props2.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application-c");
        props2.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        props2.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props2.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StandardWeatherSerde.class.getName());

        // Set up input and output topics
        String inputTopic1 = "weather-alert12";

        String inputTopic2 = "standard-weather12";

        String outputFinal = "min-temp-red-alert-weather-station";

        StreamsBuilder builder1 = new StreamsBuilder();
        // StreamsBuilder builder2 = new StreamsBuilder();

        // Read the input topic as a stream of messages
        KStream<String, WeatherAlert> inputStream1 = builder1.stream(inputTopic1,
                Consumed.with(Serdes.String(), new WeatherAlertSerde()));

        KStream<String, WeatherAlert> filteredStream1 = inputStream1
                // Filter out events that do not have a red alert
                .filter((key, value) -> value.getType().equals("red"))
                .peek((key, value) -> System.out.println("Stream-Stream Join record key " + key + " value "
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


        KStream<String, CombinedWeather> joinedStream = inputStream2.join(filteredStream1, valueJoiner,
                JoinWindows.of(Duration.ofSeconds(10)),
                StreamJoined.with(Serdes.String(), standardWeatherSerde, weatherAlertSerde));
        
                //joinedStream.groupBy((k, v) -> v.getType()).count(); 
        
        KStream<String, String> minTable = joinedStream.groupBy((k, v) -> v.getType())
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
                                                                                                Materialized.with(Serdes.String(), Serdes.Integer() )
                )
                .mapValues(v -> Integer.toString(v))
                .toStream().peek((k, v) -> System.out.println("Value: "+v));
        
        // // in a
      //  minTable
             //    .to(outputFinal, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams1 = new KafkaStreams(builder1.build(), props1);
        streams1.start();
        // KafkaStreams streams2 = new KafkaStreams(builder2.build(), props2);
        // streams2.start();
        // System.out.println("################################################################3");

    }
}
