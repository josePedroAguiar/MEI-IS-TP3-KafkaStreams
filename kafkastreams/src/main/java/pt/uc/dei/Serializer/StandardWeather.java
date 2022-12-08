package pt.uc.dei.Serializer;

import org.codehaus.jackson.annotate.JsonProperty;


public class StandardWeather {
    int temperature;
    String location;

  
    public StandardWeather() {}

    public StandardWeather(int temperature, String location) {
        this.temperature = temperature;
        this.location = location;
    }
    

    public int getTemperature() {
        return temperature;
    }
    
    public String getLocation() {
        return location;
    }

}
