package pt.uc.dei.Serializer;

public class WeatherAlert {
    String type;
    String location;

    public WeatherAlert() {}

    public WeatherAlert(String type, String location) {
        this.type = type;
        this.location = location;
    }

    public String getLocation() {
        return location;
    }
    public String getType() {
        return type;
    }
}
