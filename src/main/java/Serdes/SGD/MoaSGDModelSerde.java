package Serdes.SGD;

import mlAlgorithms.SGD.MoaSGDModel;
import mlAlgorithms.SGD.MoaSGDModelDeserializer;
import mlAlgorithms.SGD.MoaSGDModelSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;


public class MoaSGDModelSerde implements Serde<MoaSGDModel> {

    private final MoaSGDModelSerializer serializer = new MoaSGDModelSerializer();
    private final MoaSGDModelDeserializer deserializer = new MoaSGDModelDeserializer();

    @Override
    public Serializer<MoaSGDModel> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<MoaSGDModel> deserializer() {
        return deserializer;
    }
}
