package Serdes.FIMTDD;

import mlAlgorithms.FIMTDD.MoaFIMDDModelDeserializer;
import mlAlgorithms.FIMTDD.MoaFIMDDModelSerializer;
import mlAlgorithms.FIMTDD.MoaFIMTDDModel;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;



public class MoaFIMTDDModelSerde implements Serde<MoaFIMTDDModel> {

    private final MoaFIMDDModelSerializer serializer = new MoaFIMDDModelSerializer();
    private final MoaFIMDDModelDeserializer deserializer = new MoaFIMDDModelDeserializer();

    @Override
    public Serializer<MoaFIMTDDModel> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<MoaFIMTDDModel> deserializer() {
        return deserializer;
    }
}
