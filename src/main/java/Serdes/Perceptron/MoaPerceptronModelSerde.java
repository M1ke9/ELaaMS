package Serdes.Perceptron;

import mlAlgorithms.Perceptron.MoaPerceptronModel;
import mlAlgorithms.Perceptron.MoaPerceptronModelDeserializer;
import mlAlgorithms.Perceptron.MoaPerceptronModelSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;



public class MoaPerceptronModelSerde implements Serde<MoaPerceptronModel> {

    private final MoaPerceptronModelSerializer serializer = new MoaPerceptronModelSerializer();
    private final MoaPerceptronModelDeserializer deserializer = new MoaPerceptronModelDeserializer();

    @Override
    public Serializer<MoaPerceptronModel> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<MoaPerceptronModel> deserializer() {
        return deserializer;
    }
}
