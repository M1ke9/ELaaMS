package Serdes.Decision_Trees;

import mlAlgorithms.Decision_Trees.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;



public class MoaHATModelSerde implements Serde<MoaHATModel> {

    private final MoaHATModelSerializer serializer = new MoaHATModelSerializer();
    private final MoaHATModelDeserializer deserializer = new MoaHATModelDeserializer();

    @Override
    public Serializer<MoaHATModel> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<MoaHATModel> deserializer() {
        return deserializer;
    }
}

