package mlAlgorithms;

import com.yahoo.labs.samoa.instances.InstancesHeader;
import org.apache.kafka.common.serialization.Serde;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class MLAlgorithm implements Serializable {
    private static final long serialVersionUID = 1L;
    private int algorithmId;
    private String algorithmType;
   // private List<String> AlgorithmParameters;
   protected InstancesHeader instancesHeader;
   // private  String modelID;

    // The new hyperParams field
    protected Map<String,Object> hyperParams;

    public MLAlgorithm() {

    }

    public MLAlgorithm(int algorithmId, String algorithmType, Map<String,Object> hyperParams,InstancesHeader instancesHeader) {
        this.algorithmId = algorithmId;
        this.algorithmType = algorithmType;
        this.hyperParams = (hyperParams != null) ? hyperParams : new HashMap<>();
        this.instancesHeader=instancesHeader;


        //this.modelID=modelID;
    }

    // Abstract methods
    public abstract Serde<? extends MLAlgorithm> serde();

    public abstract void train(Object dataInstance);


    public abstract Object predict(Object dataInstance);



    public abstract long size();

    // Getters and setters
    public int getAlgorithmId() {
        return algorithmId;
    }

    public void setAlgorithmId(int algorithmId) {
        this.algorithmId = algorithmId;
    }

    public String getAlgorithmType() {
        return algorithmType;
    }

    public void setAlgorithmType(String algorithmType) {
        this.algorithmType=algorithmType;
    }

    // Enforce that each child must apply the hyperparams to its internal model
    protected abstract void applyHyperParams();

    // getters / setters

    public void setHyperParams(Map<String,Object> hyperParams) {
        this.hyperParams = (hyperParams != null) ? hyperParams : new HashMap<>();
        applyHyperParams(); // Let the child handle them
    }
    public Map<String,Object> getHyperParams() {
        return hyperParams;
    }

    public InstancesHeader getInstancesHeader() {
        return instancesHeader;
    }

 //   public void setModelID(String modelID){
      //  this.modelID=modelID;
   // }

   // public String getModelID(){
    //    return modelID;
   // }

}