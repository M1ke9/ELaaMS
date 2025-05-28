package Serdes.Init;

import Structure.PartialPrediction;
import com.fasterxml.jackson.core.type.TypeReference;
import Serdes.GeneralFormat.GeneralSerde;

import java.util.Map;

public class MapPartialPredictionsSerde extends GeneralSerde<Map<String,PartialPrediction>> {
    public MapPartialPredictionsSerde() {
        super(new TypeReference<Map<String,PartialPrediction>>() {});
    }
}
