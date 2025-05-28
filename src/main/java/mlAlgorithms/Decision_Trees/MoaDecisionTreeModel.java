package mlAlgorithms.Decision_Trees;

import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import mlAlgorithms.MLAlgorithm;

import moa.classifiers.trees.HoeffdingTree;


import moa.core.Utils;
import org.apache.kafka.common.serialization.Serde;
import Serdes.Decision_Trees.MoaDecisionTreeModelSerde;

//import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

public class MoaDecisionTreeModel extends MLAlgorithm implements Serializable {
   // @Serial
   // private static final long serialVersionUID = 1L;
    private HoeffdingTree classifier;

    private int instanceCount;
   // private  InstancesHeader instancesHeader;
    private boolean trainingComplete = false;

 //   Map<String,Object> hyperParams;

    public MoaDecisionTreeModel(int algorithmId, String algorithmType, Map<String,Object> hyperParams, InstancesHeader instancesHeader) {
        super(algorithmId, algorithmType,hyperParams,instancesHeader);

      //  this.hyperParams     = (hyperParams != null) ? hyperParams : new HashMap<>();
      //  this.classifier = new HoeffdingTree();
        this.classifier = new HoeffdingTree();
      //  this.classifier.numericEstimatorOption
        //classifier.gracePeriodOption.setValue(200);
      //  classifier.splitConfidenceOption.setValue(0.01);
       // classifier.tieThresholdOption.setValue(0.05);

        // Parse hyperParams and set them on the classifier:
        applyHyperParams();


       // this.instancesHeader = instancesHeader;
        this.classifier.setModelContext(this.instancesHeader);
        this.classifier.prepareForUse();
        this.instanceCount = 0;
    }

    public MoaDecisionTreeModel() {
        super();
    }

    @Override
    public Serde<MoaDecisionTreeModel> serde() {

        return new MoaDecisionTreeModelSerde();
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
        if (hyperParams.containsKey("gracePeriod")) {
            int grace = toInt(hyperParams.get("gracePeriod"));
            classifier.gracePeriodOption.setValue(grace);
        }
        if (hyperParams.containsKey("splitConfidence")) {
            double splitConf = toDouble(hyperParams.get("splitConfidence"));
            classifier.splitConfidenceOption.setValue(splitConf);
        }
        if (hyperParams.containsKey("tieThreshold")) {
            double tieTh = toDouble(hyperParams.get("tieThreshold"));
            classifier.tieThresholdOption.setValue(tieTh);
        }

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


    public HoeffdingTree getClassifier() {
        return this.classifier;
    }

    public void setClassifier(HoeffdingTree classifier) {
        this.classifier=classifier;
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
