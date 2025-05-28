
package Microservices.Router;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maintains all microservices for a given (datasetKey, streamId) pair
 * or datasetKey-only scenario.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MicroServiceInfo {

    private String datasetKey;   // new field
    private String streamId;
    // private Map<String, MLMicroserviceDetails> microservices;

    //microservices as a concurrent hashmap
    private ConcurrentHashMap<String, MicroserviceDetails> microservices;

    public MicroServiceInfo() {

        //this.microservices = new HashMap<>();
        this.microservices = new  ConcurrentHashMap<>();
    }


    /**
     * Checks if this MicroserviceInfo already contains a microservice
     * for the specified algorithm type.
     */
    public boolean hasMicroserviceKey(String microserviceKey) {
        return microservices.containsKey(microserviceKey);
    }


    public void removeMicroservice(String microserviceKey) {

       // microservices.get(algoType).setRemoved(true);
        microservices.remove(microserviceKey);
    }


    /**
     * Add a new microservice if not already present.
     */
    public void addMicroservice(String microserviceKey,String algorithmType, int algorithmId,String target,String taskType,Map<String,Object> hyperParams) {
        MicroserviceDetails mlMicroserviceDetails = new MicroserviceDetails(algorithmType, algorithmId, target,taskType,hyperParams);

      //  mlMicroserviceDetails.setMicroserviceId(streamId+"-"+datasetKey+"-"+mlMicroserviceDetails.getMicroserviceId());

        mlMicroserviceDetails.buildUniqueMicroserviceId( streamId, datasetKey);
        mlMicroserviceDetails.setRemoved(false);

        microservices.put(microserviceKey, mlMicroserviceDetails);
       // microservices.put(
            //    algorithmType,
             //   new MLMicroserviceDetails(algorithmType, algorithmId,target)
       // );

    }

    /**
     * Return all microservices in a Collection.
     */

    @JsonIgnore
    public Collection<MicroserviceDetails> getAllMicroservices() {
        return new ArrayList<>(microservices.values());
}

    // get microservices method


    // get the active microservices for specific streamId


    // Getters and setters
    public String getDatasetKey() {
        return datasetKey;
    }

    public void setDatasetKey(String datasetKey) {
        this.datasetKey = datasetKey;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public ConcurrentHashMap<String, MicroserviceDetails> getMicroservices() {
        return microservices;
    }

    public void setMicroservices(ConcurrentHashMap<String, MicroserviceDetails> microservices) {
        this.microservices = (microservices != null) ? microservices : new ConcurrentHashMap<>();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("MicroserviceInfo{");
        sb.append("datasetKey='").append(datasetKey).append("', ");
        sb.append("streamId='").append(streamId).append("', ");
        sb.append("microservices={");
        for (Map.Entry<String, MicroserviceDetails> entry : microservices.entrySet()) {
            sb.append("\n  ").append(entry.getKey()).append(": ").append(entry.getValue().toString());
        }
        sb.append("\n}}");
        return sb.toString();
    }
}
