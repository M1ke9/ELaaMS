package Serdes.Naive_Bayes;


import mlAlgorithms.Naive_Bayes.MoaNaiveBayesModel;
import mlAlgorithms.Naive_Bayes.MoaNaiveBayesModelDeserializer;
import mlAlgorithms.Naive_Bayes.MoaNaiveBayesModelSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class MoaNaiveBayesModelSerde implements Serde<MoaNaiveBayesModel> {

    private final MoaNaiveBayesModelSerializer serializer = new MoaNaiveBayesModelSerializer();
    private final MoaNaiveBayesModelDeserializer deserializer = new MoaNaiveBayesModelDeserializer();

    @Override
    public Serializer<MoaNaiveBayesModel> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<MoaNaiveBayesModel> deserializer() {
        return deserializer;
    }
}
