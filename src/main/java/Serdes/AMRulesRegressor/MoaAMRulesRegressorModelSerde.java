package Serdes.AMRulesRegressor;

import mlAlgorithms.AMRules.MoaAMRulesRegressorModel;
import mlAlgorithms.AMRules.MoaAMRulesRegressorModelDeserializer;
import mlAlgorithms.AMRules.MoaAMRulesRegressorModelSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;


public class MoaAMRulesRegressorModelSerde  implements Serde<MoaAMRulesRegressorModel> {

    private final MoaAMRulesRegressorModelSerializer serializer = new MoaAMRulesRegressorModelSerializer();
    private final MoaAMRulesRegressorModelDeserializer deserializer = new MoaAMRulesRegressorModelDeserializer();

    @Override
    public Serializer<MoaAMRulesRegressorModel> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<MoaAMRulesRegressorModel> deserializer() {
        return deserializer;
    }
}
