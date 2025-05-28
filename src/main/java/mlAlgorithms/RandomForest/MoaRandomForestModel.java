package mlAlgorithms.RandomForest;

import Serdes.RandomForest.MoaAdaptiveRandomForestSerde;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import mlAlgorithms.MLAlgorithm;
import moa.classifiers.meta.AdaptiveRandomForest;

import moa.core.Utils;
import org.apache.kafka.common.serialization.Serde;

import java.io.Serializable;
import java.util.Map;

public class MoaRandomForestModel extends MLAlgorithm implements Serializable {
    // @Serial
    // private static final long serialVersionUID = 1L;
    private AdaptiveRandomForest classifier;

    private int instanceCount;
   // private InstancesHeader instancesHeader;
    private boolean trainingComplete = false;

    //   Map<String,Object> hyperParams;

    public MoaRandomForestModel(int algorithmId, String algorithmType, Map<String,Object> hyperParams, InstancesHeader instancesHeader) {
        super(algorithmId, algorithmType,hyperParams,instancesHeader);


        this.classifier = new AdaptiveRandomForest();


        // Parse hyperParams and set them on the classifier:
        applyHyperParams();

        //this.instancesHeader = instancesHeader;
        this.classifier.setModelContext(this.instancesHeader);
        this.classifier.prepareForUse();
        this.instanceCount = 0;
    }

    public MoaRandomForestModel() {
        super();
    }

    @Override
    public Serde<MoaRandomForestModel> serde() {

        return new MoaAdaptiveRandomForestSerde();
    }
    @Override
    public void train(Object dataInstance) {
        if (dataInstance instanceof Instance) {
            this.classifier.trainOnInstance((Instance) dataInstance);
            this.instanceCount++;
        } else {
            throw new IllegalArgumentException("Data instance must be of type Instance.");
        }
    }

    @Override
    public Object predict(Object dataInstance) {
        if (dataInstance instanceof Instance) {
            double[] votes = this.classifier.getVotesForInstance((Instance) dataInstance);
            if (votes.length == 0) {
                return null;
            }
            int predictionIndex = Utils.maxIndex(votes);
            return this.instancesHeader.classAttribute().value(predictionIndex);
        } else {
            throw new IllegalArgumentException("Data instance must be of type Instance.");
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
            this.classifier.ensembleSizeOption.setValue(ensembleSizeOption);
        }
        if (hyperParams.containsKey("mFeaturesPerTreeSize")) {
            int mFeaturesPerTreeSize = toInt( hyperParams.getOrDefault("mFeaturesPerTreeSize", 2));
            this.classifier.mFeaturesPerTreeSizeOption.setValue(mFeaturesPerTreeSize);
        }


            this.classifier.numberOfJobsOption.setValue(1);


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

    public InstancesHeader getInstancesHeader() {
        return this.instancesHeader;
    }


    public AdaptiveRandomForest getClassifier() {
        return this.classifier;
    }

    public void setClassifier(AdaptiveRandomForest classifier) {
        this.classifier=classifier;
    }

    public void setInstanceCount(int instanceCount) {
        this.instanceCount=instanceCount;
    }

    public void setInstancesHeader(InstancesHeader instancesHeader) {
        this.instancesHeader=instancesHeader;
    }



}

