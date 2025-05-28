package Structure;

import java.util.Map;

public class PartialPrediction {
   // private String recordId;
   // private String streamId;
    private String algorithmType; // e.g. "Naive-Bayes"
    private String prediction;
   // private String target;

    private String hyperParams;


    // Default constructor (required for Jackson)
    public PartialPrediction() {
    }

    // Parameterized constructo
    //
    // String recordId, String streamId
    public PartialPrediction( String algorithmType, String prediction,String hyperParams){
        //this.recordId = recordId;
       // this.streamId = streamId;
        this.algorithmType = algorithmType;
        this.prediction = prediction;
       // this.target = target;
        this.hyperParams=hyperParams;
    }
/*
    // Getters and setters
    public String getRecordId() {
        return recordId;
    }
    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }
    public String getStreamId() {
        return streamId;
    }
    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

 */


    public String getAlgorithmType() {
        return algorithmType;
    }
    public void setAlgorithmType(String algorithmType) {
        this.algorithmType = algorithmType;
    }
    public String getPrediction() {
        return prediction;
    }
    /*
    public void setPrediction(String prediction) {
        this.prediction = prediction;
    }

    public String getTarget() {
        return target;
    }

     */
/*
    public void setTarget(String target) {
        this.target = target;
    }


 */

    public String getHyperParams() {
        return hyperParams;
    }
    public void setHyperParams(String hyperParams) {
        this.hyperParams = hyperParams;
    }


    @Override
    public String toString() {
        return "PartialPrediction{" +
               // "recordId='" + recordId + '\'' +
               // ", streamId='" + streamId + '\'' +
                ", hyperparams:" + hyperParams + '\'' +
                ", algorithmType='" + algorithmType + '\'' +
                ", predictedLabel='" + prediction + '\'' +
                '}';
    }
}
