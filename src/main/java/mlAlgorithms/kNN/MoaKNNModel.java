package mlAlgorithms.kNN;

import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import moa.classifiers.lazy.kNN;
import moa.core.Utils;
import org.apache.kafka.common.serialization.Serde;
import mlAlgorithms.MLAlgorithm;
import Serdes.kNN.MoaKNNModelSerde;

//import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

public class MoaKNNModel extends MLAlgorithm implements Serializable {
 //   @Serial
   // private static final long serialVersionUID = 1L;
    private kNN learner;
    private int instanceCount;
   // private InstancesHeader instancesHeader;
    private boolean trainingComplete = false;


    public MoaKNNModel(int algorithmId, String algorithmType , Map<String,Object> hyperParams, InstancesHeader instancesHeader) {
        super(algorithmId, algorithmType, hyperParams,instancesHeader);
        this.learner = new kNN();
        applyHyperParams();
        //this.instancesHeader = instancesHeader;
        this.learner.setModelContext(this.instancesHeader);
        this.learner.prepareForUse();
        this.instanceCount = 0;
      //  System.out.println("KNN k "+ classifier.get)
    }

    public MoaKNNModel() {
        super();
    }

    @Override
    public Serde<MoaKNNModel> serde() {
        // If you have a specialized SerDe for Naive Bayes, use that. For now, using the decision tree one as placeholder.
        return new MoaKNNModelSerde();
    }

    @Override
    public void train(Object dataInstance) {
        if (dataInstance instanceof Instance) {
            this.learner.trainOnInstance((Instance) dataInstance);
            this.instanceCount++;
        } else {
            throw new IllegalArgumentException("Data instance must be of type Instance.");
        }
    }



    /*
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

     */

    @Override
    public Object predict(Object dataInstance) {
        if (!(dataInstance instanceof Instance))
            throw new IllegalArgumentException("Data instance must be Instance");

        double[] votes = learner.getVotesForInstance((Instance) dataInstance);
        if (votes.length == 0) return null;

        // ---------- NEW: branch by attribute type ----------
        if (instancesHeader.classAttribute().isNominal()) {          // classification
            int idx = Utils.maxIndex(votes);
            return instancesHeader.classAttribute().value(idx);
        } else {                                                     // regression
            return votes[0];  // numeric prediction
        }
    }



    @Override
    public long size() {
        return 0;
    }

    @Override
    protected void applyHyperParams() {

        if (hyperParams.containsKey("k")) {
            int k = toInt(hyperParams.get("k"));
            this.learner.kOption.setValue(k);
        }

        if (hyperParams.containsKey("nearestNeighbourSearchOption")) {
            int nearestNeighbourSearchOption = toInt(hyperParams.get("nearestNeighbourSearchOption"));
            this.learner.nearestNeighbourSearchOption.setChosenIndex(nearestNeighbourSearchOption);
            //  this.classifier.medianOption
            // this.classifier.nearestNeighbourSearchOption.
        }

        if (hyperParams.containsKey("limitOption")) {
            int limitOption = toInt(hyperParams.get("limitOption"));
            this.learner.limitOption.setValue(limitOption);

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

    // Additional methods if needed

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

    public kNN getLearner() {
        return this.learner;
    }

    public void setLearner(kNN learner) {
        this.learner = learner;
    }

    public void setInstanceCount(int instanceCount) {
        this.instanceCount = instanceCount;
    }
/*
    public void setInstancesHeader(InstancesHeader instancesHeader) {
        this.instancesHeader = instancesHeader;
    }

 */
}