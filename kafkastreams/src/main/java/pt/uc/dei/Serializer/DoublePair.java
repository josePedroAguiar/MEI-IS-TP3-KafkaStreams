package pt.uc.dei.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class DoublePair implements Deserializer<  DoublePair >{
    private double min;
    private double max;

    public DoublePair() {
        this.min = Double.MAX_VALUE;
        this.max = Double.MIN_VALUE;
    }
    /**
     * @param a
     * @param b
     */
    public DoublePair(double a,double b) {
        this.min = a;
        this.max = b;
    }

    public void update(double value) {
        this.min = Math.min(this.min, value);
        this.max = Math.max(this.max, value);
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no configuration required
    }

    @Override
    public DoublePair deserialize(String topic, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        double first = buffer.getDouble();
        double second = buffer.getDouble();
        return new DoublePair(first, second);

    }

    @Override
    public void close() {
        // no cleanup required
    }
}
