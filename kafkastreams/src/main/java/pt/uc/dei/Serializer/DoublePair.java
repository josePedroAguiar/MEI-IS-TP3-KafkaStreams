package pt.uc.dei.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class DoublePair{
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
}
