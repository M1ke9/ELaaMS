package Structure;

import java.util.List;
import java.util.Map;

public class DataStructure {
    private String streamID;
    private String dataSetKey;

    private String recordID;
   // private Map<String,Object> data;
    // private List<String> features;
  //  private String target;
   private Map<String, Object> fields;

    public DataStructure() {
    }

    // Getters and Setters


    public String getStreamID() {
        return streamID;
    }

    public void setStreamID(String streamID) {
        this.streamID = streamID;
    }

    public String getDataSetKey() {
        return dataSetKey;
    }

    public void setDataSetKey(String dataSetKey) {
        this.dataSetKey = dataSetKey;
    }

    public String getRecordID() {
        return recordID;
    }

    public void setRecordID(String recordID) {
        this.recordID = recordID;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }





}
