package Serdes.GeneralFormat;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.serialization.Serdes;


public class GeneralSerde<T> extends Serdes.WrapperSerde<T> {



    public GeneralSerde(Class<T> clazz) {
        super(new GeneralSerializer<>(), new GeneralDeserializer<>(clazz));
    }

    public GeneralSerde(TypeReference<T> typeReference) {
        super(new GeneralSerializer<>(), new GeneralDeserializer<>(typeReference));
    }


}