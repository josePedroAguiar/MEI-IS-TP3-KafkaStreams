package pt.uc.dei.Serializer;

import org.codehaus.jackson.annotate.JsonProperty;



public class WeatherAlert {
    @JsonProperty
    String type;
    @JsonProperty
    String location;

    public WeatherAlert() {}

    public WeatherAlert(String type, String location) {
        this.type = type;
        this.location = location;
    }

    public Object getWeatherStation() {
        return null;
    }

    public Object getTemperature() {
        return null;
    }

}
