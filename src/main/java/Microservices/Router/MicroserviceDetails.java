package Microservices.Router;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;


/**
 * Represents the details of one ML microservice (algorithm, ID, etc.)
 */
public class MicroserviceDetails {

    private String algorithmType;
    private int algorithmId;
    private String microserviceId;

    private String target;

    private String taskType;

    private boolean removed;

    private Map<String, Object> hyperParams;

    // Default constructor for Jackson
    public MicroserviceDetails() {
    }

    @JsonCreator
    public MicroserviceDetails(
            @JsonProperty("algorithmType") String algorithmType,
            @JsonProperty("algorithmId") int algorithmId,
            @JsonProperty("target") String target,
            @JsonProperty("taskType") String taskType,
            @JsonProperty("hyperParams") Map<String,Object> hyperParams // add this
    ) {
        this.algorithmType = algorithmType;
        this.algorithmId = algorithmId;
        this.target = target;
        this.taskType=taskType;
        this.hyperParams   = (hyperParams == null) ? new HashMap<>() : hyperParams;
        this.microserviceId = algorithmType + "-" + algorithmId + "-" + target;
        this.removed = false;
    }


    public Map<String, Object> getHyperParams() {
        return hyperParams;
    }
    public void setHyperParams(Map<String, Object> hyperParams) {
        this.hyperParams = hyperParams;
    }

    // Getters and setters
    public String getAlgorithmType() {
        return algorithmType;
    }

    public void setAlgorithmType(String algorithmType) {
        this.algorithmType = algorithmType;
    }

    public int getAlgorithmId() {
        return algorithmId;
    }



    public void setAlgorithmId(int algorithmId) {
        this.algorithmId = algorithmId;
    }

    public String getTarget() {
        return target;
    }
    public void setTarget(String target) {
        this.target = target;
    }

    public String getMicroserviceId() {
        return microserviceId;
    }

    public void setMicroserviceId(String microserviceId) {
        this.microserviceId = microserviceId;
    }

    @Override
    public String toString() {
        return "MLMicroserviceDetails{" +
                "algorithmType='" + algorithmType + '\'' +
                ", algorithmId=" + algorithmId +
                ", microserviceId='" + microserviceId + '\'' +
                ", isRemoved='" + isRemoved() + '\'' +
                '}';
    }

    public boolean isRemoved() {
        return removed;
    }

    public void setRemoved(boolean removed) {
        this.removed = removed;
    }

    public void setTaskType(String taskType){
        this.taskType=taskType;

    }

    public String getTaskType(){
        return this.taskType;
    }

    public void buildUniqueMicroserviceId(String streamID, String datasetKey) {
        String paramSig = buildParamSignature(hyperParams);
        String dataKey = null;

        if (streamID != null && !streamID.isEmpty()) {
            dataKey=  streamID + "-" + datasetKey;
        } else {
            dataKey =datasetKey;
        }
        this.microserviceId = dataKey+"-"+algorithmType + "-" + algorithmId + "-" + target + "-" + paramSig;
    }

    // Example approach:
    private static String buildParamSignature(Map<String, Object> hyperParams) {
        if (hyperParams == null || hyperParams.isEmpty()) {
            return "defaultParams";
        }
        // Quick approach: convert to JSON or sorted string. E.g.:
        try {
            // using Jackson
            return new ObjectMapper().writeValueAsString(hyperParams).hashCode() + "";
        } catch (Exception e) {
            // fallback
            return hyperParams.toString().hashCode() + "";
        }
    }

}
