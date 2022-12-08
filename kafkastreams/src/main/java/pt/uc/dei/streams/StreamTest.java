package pt.uc.dei.streams;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList; // import the ArrayList class
import java.util.List; // import the ArrayList class

import pt.uc.dei.Serializer.WeatherAlert;
import pt.uc.dei.Serializer.WeatherAlertSerde;

public class StreamTest {

    public static void main(String[] args) throws InterruptedException, IOException {
        if (args.length != 2) {
            System.err.println("Wrong arguments. Please run the class as follows:");
            System.err.println(SimpleStreamsExercisesa.class.getName() + " input-topic output-topic");
            System.exit(1);
        }
        String topicName = args[0].toString();
        String outtopicname = args[1].toString();

        java.util.Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application-a");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        //KStream<String, JSONObject> lines = builder.stream(topicName);

        /*
         * KStream<String, WeatherAlert> weather = lines.flatMap((k, v) -> {
         * List<KeyValue<String,WeatherAlert>> tmp = new
         * ArrayList<KeyValue<String,WeatherAlert>>();
         * WeatherAlert alert = new WeatherAlert(v.getString("type"),
         * v.getString("location"));
         * tmp.add(new KeyValue(k, alert));
         * return tmp;
         * });
         */

         KStream<String, JSONObject> lines = builder.stream(topicName);
         KStream<String, WeatherAlert> weather = lines.flatMap((k, v) -> {
             List<KeyValue<String,WeatherAlert>> tmp = new ArrayList<KeyValue<String,WeatherAlert>>();
             WeatherAlert alert = new WeatherAlert(v.getString("type"), v.getString("location"));
             tmp.add(new KeyValue(k, alert));
             return tmp;
         });

        KTable<String, Long> outlines = weather.groupByKey().count();

        outlines.toStream().to(outtopicname);
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        System.out.println("Reading stream from topic " + topicName);
    }
}