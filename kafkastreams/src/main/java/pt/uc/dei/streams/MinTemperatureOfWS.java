package pt.uc.dei.streams;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
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
import org.apache.kafka.streams.kstream.ValueJoiner;

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
        

        // Create a Kafka Streams configuration object
        Properties props2 = new Properties();
        props2.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application-b");
        props2.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        props2.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props2.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StandardWeatherSerde.class.getName());
        

        // Set up input and output topics
        String inputTopic1 = "weather-alert11";

        String inputTopic2 = "standard-weather11";

        String outputFinal = "min-temp-red-alert-weather-station";

        StreamsBuilder builder1 = new StreamsBuilder();
        //StreamsBuilder builder2 = new StreamsBuilder();


        // Read the input topic as a stream of messages
        KStream<String, WeatherAlert> inputStream1 = builder1.stream(inputTopic1,
                Consumed.with(Serdes.String(), new WeatherAlertSerde()));

        KStream<String, String> filteredStream1 = inputStream1
                // Filter out events that do not have a red alert
                .filter((key, value) -> value.getType().equals("red")).mapValues(v->v.toString())
                
                ;



        // Read the input topic as a stream of messages
        KStream<String, String> inputStream2 = builder1.stream(inputTopic2,
        Consumed.with(Serdes.String(), new StandardWeatherSerde())).mapValues(v->v.toString())
        ;


        ValueJoiner<String,String,String> valueJoiner=(leftValue,rightValue)->{
            System.out.println(leftValue+rightValue);
            return leftValue+rightValue;
        };
        filteredStream1.join(inputStream2,valueJoiner,JoinWindows.of(Duration.ofSeconds(10)));

    

        KafkaStreams streams1 = new KafkaStreams(builder1.build(), props1);
        streams1.start();
        // KafkaStreams streams2 = new KafkaStreams(builder2.build(), props2);
        // streams2.start();
        // System.out.println("################################################################3");

    }
}
