package Serdes.GeneralFormat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class GeneralDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JavaType type;

    public GeneralDeserializer(Class<T> type) {
        this.type = objectMapper.getTypeFactory().constructType(type);
    }

    public GeneralDeserializer(TypeReference<T> type) {
        this.type = objectMapper.getTypeFactory().constructType(type);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, type);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}