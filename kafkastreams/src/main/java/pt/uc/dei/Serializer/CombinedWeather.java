package pt.uc.dei.Serializer;

public class CombinedWeather {
    String type;
    int temperature;
    String location;

    public CombinedWeather() {}

    public CombinedWeather(String type, int temperature, String location) {
        this.type = type;
        this.temperature =  temperature;
        this.location = location;
    }
    public void setLocation(String location) {
        this.location = location;
    }
    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }
    public void setType(String type) {
        this.type = type;
    }

    public int getTemperature() {
        return temperature;
    }
    
    public String getLocation() {
        return location;
    }
    public String getType() {
        return type;
    }
}
