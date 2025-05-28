package mlAlgorithms.RandomForest;

import Serdes.RandomForest.MoaAdaptiveRandomForestSerde;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import mlAlgorithms.MLAlgorithm;
import moa.classifiers.meta.AdaptiveRandomForestRegressor;
import org.apache.kafka.common.serialization.Serde;

import java.io.Serializable;
import java.util.Map;

public class MoaRandomForestRegressorModel extends MLAlgorithm implements Serializable {
    // @Serial
    // private static final long serialVersionUID = 1L;
    private AdaptiveRandomForestRegressor regressor;

    private int instanceCount;
   // private InstancesHeader instancesHeader;
    private boolean trainingComplete = false;

    //   Map<String,Object> hyperParams;

    public MoaRandomForestRegressorModel(int algorithmId, String algorithmType, Map<String,Object> hyperParams, InstancesHeader instancesHeader) {
        super(algorithmId, algorithmType,hyperParams,instancesHeader);


        this.regressor = new AdaptiveRandomForestRegressor();


        // Parse hyperParams and set them on the classifier:
        applyHyperParams();

      //  this.instancesHeader = instancesHeader;
        this.regressor.setModelContext(this.instancesHeader);
        this.regressor.prepareForUse();
        this.instanceCount = 0;
    }

    public MoaRandomForestRegressorModel() {
        super();
    }

    @Override
    public Serde<MoaRandomForestModel> serde() {

        return new MoaAdaptiveRandomForestSerde();
    }
    @Override
    public void train(Object dataInstance) {
        if (dataInstance instanceof Instance) {
            this.regressor.trainOnInstance((Instance) dataInstance);
            this.instanceCount++;
        } else {
            throw new IllegalArgumentException("Data instance must be of type Instance.");
        }
    }

    @Override
    public Object predict(Object dataInstance) {
        if (!(dataInstance instanceof Instance)) {
            throw new IllegalArgumentException("Data instance must be Instance");
        }
        try {
            double[] votes = regressor.getVotesForInstance((Instance) dataInstance);
            if (votes == null || votes.length == 0) {
                return null;
            }
            return votes[0];
        } catch (NullPointerException npe) {
            // happens when an internal tree node is missing → skip this record
            return null;
        }
    }




    @Override
    public long size() {
        // Return the size of the model, e.g., number of nodes in the tree
        return 0;
    }

    @Override
    protected void applyHyperParams() {

        if (hyperParams.containsKey("ensembleSize")) {
            int ensembleSizeOption = toInt( hyperParams.getOrDefault("ensembleSize", 2));
            this.regressor.ensembleSizeOption.setValue(ensembleSizeOption);
        }



        //  this.classifier.numberOfJobsOption.setValue(1);
        regressor.setRandomSeed(42);

        regressor.mFeaturesModeOption.setChosenIndex(1);  // Square root of features




    // regressor.ensembleSizeOption.setValue(10);           // Increase ensemble size (15-30 typical)




    }



    private int toInt(Object val) {
        if (val instanceof Number) return ((Number)val).intValue();
        return Integer.parseInt(val.toString());
    }
    private double toDouble(Object val) {
        if (val instanceof Number) return ((Number)val).doubleValue();
        return Double.parseDouble(val.toString());
    }

    // Existing methods
    public int getInstanceCount() {
        return this.instanceCount;
    }

    public boolean isTrained(int minInstances) {
        return this.instanceCount >= minInstances;
    }

    public boolean isTrainingComplete() {
        return this.trainingComplete;
    }

    public void setTrainingComplete(boolean trainingComplete) {
        this.trainingComplete = trainingComplete;
    }
/*
    public InstancesHeader getInstancesHeader() {
        return this.instancesHeader;
    }

 */


    public AdaptiveRandomForestRegressor getRegressor() {
        return this.regressor;
    }

    public void setRegressor(AdaptiveRandomForestRegressor regressor) {
        this.regressor=regressor;
    }

    public void setInstanceCount(int instanceCount) {
        this.instanceCount=instanceCount;
    }
/*
    public void setInstancesHeader(InstancesHeader instancesHeader) {
        this.instancesHeader=instancesHeader;
    }


 */


}


