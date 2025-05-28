package Serdes;

import mlAlgorithms.MLAlgorithm;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

public class MLAlgorithmSerializer implements Serializer<MLAlgorithm> {

    private final MLAlgorithmSerde innerSerde = new MLAlgorithmSerde();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no-op
    }

    @Override
    public byte[] serialize(String topic, MLAlgorithm data) {
        // Re-use the logic from your Serde
        if (data == null) return null;
        return innerSerde.serializer().serialize(topic, data);
    }

    @Override
    public void close() {
        // no-op
    }
}
