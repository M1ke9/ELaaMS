package Serdes.RandomForest;

import mlAlgorithms.RandomForest.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;



public class MoaAdaptiveRandomForestRegressorSerde implements Serde<MoaRandomForestRegressorModel> {

    private final MoaRandomForestRegressorSerializer serializer = new MoaRandomForestRegressorSerializer();
    private final MoaRandomForestRegressorDeserializer deserializer = new MoaRandomForestRegressorDeserializer();

    @Override
    public Serializer<MoaRandomForestRegressorModel> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<MoaRandomForestRegressorModel> deserializer() {
        return deserializer;
    }
}
