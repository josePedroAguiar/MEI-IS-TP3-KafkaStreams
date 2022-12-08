package pt.uc.dei.Serializer;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StandardWeatherSerde implements Serde<StandardWeather>, Serializer<StandardWeather> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nothing to configure
    }

    @Override
    public void close() {
        // Nothing to close
    }

    @Override
    public Serializer<StandardWeather> serializer() {
        return new Serializer<StandardWeather>() {

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                // Nothing to configure
            }

            @Override
            public byte[] serialize(String topic, StandardWeather data) {
                try {
                    return objectMapper.writeValueAsBytes(data);
                } catch (JsonProcessingException e) {
                    throw new SerializationException("Error serializing StandardWeather object", e);
                }
            }
            

            @Override
            public void close() {
                // Nothing to close
            }
        };
    }

    @Override
    public Deserializer<StandardWeather> deserializer() {
        return new Deserializer<StandardWeather>() {

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                // Nothing to configure
            }

            @Override
            public StandardWeather deserialize(String topic, byte[] data) {
                try {
                    return objectMapper.readValue(data, StandardWeather.class);
                } catch (IOException e) {
                    throw new SerializationException("Error deserializing StandardWeather object", e);
                }
            }

            @Override
            public void close() {
                // Nothing to close
            }
        };
    }

    @Override
    public byte[] serialize(String topic, StandardWeather data) {
        // TODO Auto-generated method stub
        return null;
    }

}