package Serdes.Init;

import Structure.DataStructure;
import Serdes.GeneralFormat.GeneralDeserializer;

public class DataStructureDeserializer extends GeneralDeserializer<DataStructure> {

    public DataStructureDeserializer() {
        super(DataStructure.class);
    }
    public DataStructureDeserializer(Class<DataStructure> type) {
        super(type);
    }
}