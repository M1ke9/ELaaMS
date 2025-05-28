package Serdes.Decision_Trees;

import mlAlgorithms.Decision_Trees.MoaDecisionTreeModel;
import mlAlgorithms.Decision_Trees.MoaDecisionTreeModelDeserializer;
import mlAlgorithms.Decision_Trees.MoaDecisionTreeModelSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class MoaDecisionTreeModelSerde implements Serde<MoaDecisionTreeModel> {

    private final MoaDecisionTreeModelSerializer serializer = new MoaDecisionTreeModelSerializer();
    private final MoaDecisionTreeModelDeserializer deserializer = new MoaDecisionTreeModelDeserializer();

    @Override
    public Serializer<MoaDecisionTreeModel> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<MoaDecisionTreeModel> deserializer() {
        return deserializer;
    }
}
