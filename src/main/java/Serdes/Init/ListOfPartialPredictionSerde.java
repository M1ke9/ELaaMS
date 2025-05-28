package Serdes.Init;

import Structure.PartialPrediction;
import com.fasterxml.jackson.core.type.TypeReference;
import Serdes.GeneralFormat.GeneralSerde;

import java.util.List;

public class ListOfPartialPredictionSerde extends GeneralSerde<List<PartialPrediction>> {
    public ListOfPartialPredictionSerde() {
        super(new TypeReference<List<PartialPrediction>>() {});
    }
}
