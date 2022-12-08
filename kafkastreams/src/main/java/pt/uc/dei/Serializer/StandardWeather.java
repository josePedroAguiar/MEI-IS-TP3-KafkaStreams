package pt.uc.dei.Serializer;

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

    public int getTemperature() {
        return 0;
    }

    public String getLocation() {
        return null;
    }

}
