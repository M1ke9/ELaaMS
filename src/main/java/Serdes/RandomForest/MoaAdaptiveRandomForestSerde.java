package Serdes.RandomForest;

import mlAlgorithms.RandomForest.MoaRandomForestDeserializer;
import mlAlgorithms.RandomForest.MoaRandomForestModel;
import mlAlgorithms.RandomForest.MoaRandomForestSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class MoaAdaptiveRandomForestSerde implements Serde<MoaRandomForestModel> {

    private final MoaRandomForestSerializer serializer = new MoaRandomForestSerializer();
    private final MoaRandomForestDeserializer deserializer = new MoaRandomForestDeserializer();

    @Override
    public Serializer<MoaRandomForestModel> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<MoaRandomForestModel> deserializer() {
        return deserializer;
    }
}
