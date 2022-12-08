package pt.uc.dei.Serializer;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StandardWeatherSerde
        implements Serde<StandardWeather> {

    final private Serializer<StandardWeather> serializer;
    final private Deserializer<StandardWeather> deserializer;

    public StandardWeatherSerde(Serializer<StandardWeather> serializer, Deserializer<StandardWeather> deserializer) {
                this.serializer = serializer;
                this.deserializer = deserializer;
            }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<StandardWeather> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<StandardWeather> deserializer() {
        return deserializer;
    }

}
