package pt.uc.dei.Serializer;


public class StandardWeather {
    int temperature;
    String location;
    public StandardWeather() {
    }

    public StandardWeather(int temperature, String location) {
        this.temperature = temperature;
        this.location = location;
    }

    public void setLocation(String location) {
        this.location = location;
    }
    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }
    public int getTemperature() {
        return temperature;
    }

    public String getLocation() {
        return location;
    }

}
