package pt.uc.dei.streams;


import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class IntArraySerde implements Serde<int[]> {

    @Override
    public Serializer<int[]> serializer() {
        return new Serializer<int[]>() {
            @Override
            public byte[] serialize(String string, int [] array) {
                /* from https://stackoverflow.com/questions/11665147/convert-a-longbuffer-intbuffer-shortbuffer-to-bytebuffer */
                ByteBuffer bb = ByteBuffer.allocate(array.length * 4);
                bb.asIntBuffer().put(array);
                return bb.array();
            }
        };
    }

    @Override
    public Deserializer<int[]> deserializer() {
        return new Deserializer<int[]>() {
            @Override
            public int[] deserialize(String string, byte[] bytes) {
                /* from https://stackoverflow.com/questions/11437203/how-to-convert-a-byte-array-to-an-int-array */
                if (bytes == null)
                    return null;
                IntBuffer intBuf = ByteBuffer.wrap(bytes).asIntBuffer();
                int[] array = new int[intBuf.remaining()];
                intBuf.get(array);
                return array;
            }
        };
    }
}
