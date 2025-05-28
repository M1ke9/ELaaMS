package Serdes.kNN;


import mlAlgorithms.kNN.MoaKNNModel;
import mlAlgorithms.kNN.MoaKNNModelDeserializer;
import mlAlgorithms.kNN.MoaKNNModelSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class MoaKNNModelSerde implements Serde<MoaKNNModel> {

    private final MoaKNNModelSerializer serializer = new MoaKNNModelSerializer();
    private final MoaKNNModelDeserializer deserializer = new MoaKNNModelDeserializer();

    @Override
    public Serializer<MoaKNNModel> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<MoaKNNModel> deserializer() {
        return deserializer;
    }
}
