package pt.uc.dei.Serializer;
import org.json.JSONPropertyName;

import lombok.Builder;
import lombok.Data;
import org.codehaus.jackson.annotate.JsonProperty;


public class StandardWeather {
    @JsonProperty
    int temperature;
    @JsonProperty
    String location;

    public StandardWeather() {}

    public StandardWeather(int temperature, String location) {
        this.temperature = temperature;
        this.location = location;
    }

}
