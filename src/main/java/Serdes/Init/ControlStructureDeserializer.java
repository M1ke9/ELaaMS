package Serdes.Init;



import Structure.ControlStructure;
import Serdes.GeneralFormat.GeneralDeserializer;

public class ControlStructureDeserializer extends GeneralDeserializer<ControlStructure> {

    public ControlStructureDeserializer() {
        super(ControlStructure.class);
    }
    public ControlStructureDeserializer(Class<ControlStructure> type) {
        super(type);
    }
}