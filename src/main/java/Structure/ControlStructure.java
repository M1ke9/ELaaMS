package Structure;

import java.util.Map;

public class ControlStructure {
    private String commandType;
    private int algorithmID;
    private String algorithmType;
    private String streamID;
    private String dataSetKey;

   // private List<String> parameters;
   private Map<String, Object> hyperParams;

    private String target;

    private String taskType;

   // private transient MicroServiceInfo existingMembership;



    public ControlStructure() {
    }

    public ControlStructure(String commandType, int algorithmID, String algorithmType, String streamID, String dataSetKey,Map<String, Object> hyperParams, String target,String  taskType) {
        this.commandType = commandType;
        this.algorithmID = algorithmID;
        this.algorithmType = algorithmType;
        this.streamID = streamID;
        this.dataSetKey = dataSetKey;
        this.hyperParams=hyperParams;
        this.target = target;
        this.taskType=taskType;
    }

    public String getCommandType() {
        return commandType;
    }


    public void setCommandType(String commandType) {
        this.commandType = commandType;
    }


    public int getAlgorithmID() {
        return algorithmID;
    }


    public void setAlgorithmID(int algorithmID) {
        this.algorithmID = algorithmID;
    }


    public String getAlgorithmType() {
        return algorithmType;
    }


    public void setAlgorithmType(String algorithmType) {
        this.algorithmType = algorithmType;
    }


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



    public Map<String, Object> getHyperParams() {
        return hyperParams;
    }
    public void setHyperParams(Map<String, Object> hyperParams) {
        this.hyperParams = hyperParams;
    }


    public String getTarget() {
        return target;
    }


    public void setTarget(String target) {
        this.target = target;
    }




    public String getTaskType()
    {
        return this.taskType;
    }

    public void setTaskType(String taskType){

        this.taskType=taskType;

    }




}
