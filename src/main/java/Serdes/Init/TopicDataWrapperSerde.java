package Serdes.Init;

import Structure.DataStructure;
import helperClasses.TopicDataWrapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A custom Serde that writes only the DataStructure portion of TopicDataWrapper.
 * "topicName" is never serialized, so the final message on the wire has
 *   key = (string, e.g. streamId)
 *   value = (the raw DataStructure bytes)
 */
public class TopicDataWrapperSerde implements Serde<TopicDataWrapper> {

    // We re-use your existing DataStructureSerde to handle DataStructure
    private final DataStructureSerde dataSerde = new DataStructureSerde();

    @Override
    public Serializer<TopicDataWrapper> serializer() {
        return new Serializer<TopicDataWrapper>() {
            @Override
            public byte[] serialize(String topic, TopicDataWrapper dataWrapper) {
                if (dataWrapper == null) {
                    return null;
                }
                // Only serialize the "data" field (DataStructure) ignoring the topicName
                return dataSerde.serializer().serialize(topic, dataWrapper.getData());
            }
        };
    }

    @Override
    public Deserializer<TopicDataWrapper> deserializer() {
        return new Deserializer<TopicDataWrapper>() {
            @Override
            public TopicDataWrapper deserialize(String topic, byte[] bytes) {
                if (bytes == null) {
                    return null;
                }
                // We read the raw DataStructure, and ignore the 'topicName'
                DataStructure ds = dataSerde.deserializer().deserialize(topic, bytes);
                // If you ever consume these topics, the topicName is lost, so set it null:
                return new TopicDataWrapper(null, ds);
            }
        };
    }
}
