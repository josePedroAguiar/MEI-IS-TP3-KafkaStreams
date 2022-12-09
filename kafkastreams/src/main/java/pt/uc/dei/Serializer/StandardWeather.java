package pt.uc.dei.Serializer;

import org.codehaus.jackson.annotate.JsonProperty;

public class StandardWeather {
    int temperature;
    String location;
    int min;
    int max;

    public StandardWeather() {
    }

    public StandardWeather(int temperature, String location) {
        this.temperature = temperature;
        this.location = location;
    }

    public StandardWeather(int min, int max) {
        this.min = min;
        this.max = max;
    }

    public int getMax() {
        return max;
    }

    public int getMin() {
        return min;
    }

    public void setMax(int max) {
        this.max = max;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public int getTemperature() {
        return temperature;
    }

    public String getLocation() {
        return location;
    }

}
