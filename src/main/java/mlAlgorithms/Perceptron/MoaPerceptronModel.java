package mlAlgorithms.Perceptron;

import Serdes.Perceptron.MoaPerceptronModelSerde;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import mlAlgorithms.MLAlgorithm;
import moa.classifiers.functions.Perceptron;
import moa.core.Utils;
import org.apache.kafka.common.serialization.Serde;

import java.io.Serializable;
import java.util.Map;

public class MoaPerceptronModel extends MLAlgorithm implements Serializable {
    // @Serial
    // private static final long serialVersionUID = 1L;
    private Perceptron learner;

    private int instanceCount;
   // private  InstancesHeader instancesHeader;
    private boolean trainingComplete = false;

    //   Map<String,Object> hyperParams;

    public MoaPerceptronModel(int algorithmId, String algorithmType, Map<String,Object> hyperParams, InstancesHeader instancesHeader) {
        super(algorithmId, algorithmType,hyperParams,instancesHeader);


        this.learner = new Perceptron();


        // Parse hyperParams and set them on the classifier:
        applyHyperParams();

       // this.instancesHeader = instancesHeader;
        this.learner.setModelContext(this.instancesHeader);
        this.learner.prepareForUse();
        this.instanceCount = 0;
    }

    public MoaPerceptronModel() {
        super();
    }


        @Override
        public Serde<MoaPerceptronModel> serde() {

            return new MoaPerceptronModelSerde();
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
        // Return the size of the model, e.g., number of nodes in the tree
        return 0;
    }

    @Override
    protected void applyHyperParams() {

        if(hyperParams.containsKey("learningRatio"))
        {
            double learningRatio = toDouble(hyperParams.get("learningRatio"));
            this.learner.learningRatioOption.setValue(learningRatio);
            //  this.classifier
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


    public Perceptron getLearner() {
        return this.learner;
    }

    public void setLearner(Perceptron learner) {
        this.learner=learner;
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
