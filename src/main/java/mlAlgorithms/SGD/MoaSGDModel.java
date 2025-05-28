package mlAlgorithms.SGD; // or wherever you keep your MLAlgorithm classes

import Serdes.SGD.MoaSGDModelSerde;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import moa.classifiers.functions.SGD;
import org.apache.kafka.common.serialization.Serde;
import mlAlgorithms.MLAlgorithm;
import java.io.Serializable;
import java.util.Map;

/**
 * A wrapper for the MOA SGD classifier, configured for regression via squared loss.
 */
public class MoaSGDModel extends MLAlgorithm implements Serializable {

    // The MOA SGD learner
    private SGD learner;

    // We store the InstancesHeader for referencing the schema
   // private InstancesHeader instancesHeader;

    // Keep track of how many training instances we've processed
    private int instanceCount;

    private boolean trainingComplete = false;
    public MoaSGDModel(int algorithmId, String algorithmType, Map<String,Object> hyperParams, InstancesHeader instancesHeader) {
        super(algorithmId, algorithmType, hyperParams,instancesHeader);
        this.instanceCount = 0;

        // 1) Create the MOA SGD object
        learner = new SGD();

        applyHyperParams();

        // Optionally tweak other hyperparams:
        // sgd.learningRateOption.setValue(0.0001);
        // sgd.epochsOption.setValue(1);
        // sgd.lambdaOption.setValue(1e-6);

        // 3) Provide the InstancesHeader
        //this.instancesHeader = instancesHeader;
        learner.setModelContext(this.instancesHeader);
        learner.prepareForUse();
    }

    // A no-arg constructor in case you need it for serialization
    public MoaSGDModel() {}

    @Override
    public Serde<MoaSGDModel> serde() {

          return new MoaSGDModelSerde();

    }

    @Override
    public void train(Object dataInstance) {
        if (dataInstance instanceof Instance) {
            learner.trainOnInstance((Instance) dataInstance);
            instanceCount++;
        } else {
            throw new IllegalArgumentException("Data instance must be of type Instance.");
        }
    }
/*
    @Override
    public Object predict(Object dataInstance) {

        if (dataInstance instanceof Instance)
        {
            if(sgd.lossFunctionOption.getChosenIndex()==1) {
                double[] votes = this.sgd.getVotesForInstance((Instance) dataInstance);
                if (votes.length == 0) {
                    return null;
                }
                int predictionIndex = Utils.maxIndex(votes);
                return this.instancesHeader.classAttribute().value(predictionIndex);
            }
            else if(sgd.lossFunctionOption.getChosenIndex()==2) {

                double[] votes = sgd.getVotesForInstance((Instance) dataInstance);

                // For regression, MOA’s getVotesForInstance usually returns an array of length 1:
                if (votes.length == 0) {
                    return null;
                }
                // The numeric prediction
                double predictedValue = votes[0];

                // You can return it as Double or a string.
                // The rest of your code might do .toString() anyway.
                return predictedValue;
            }
        } else {
            throw new IllegalArgumentException("Data instance must be of type Instance.");
        }

    }

 */
@Override
    public Object predict(Object dataInstance) {
        if (!(dataInstance instanceof Instance)) {
            throw new IllegalArgumentException("Data instance must be of type Instance.");
        }

        // Get the numeric votes from MOA's SGD
        double[] votes = learner.getVotesForInstance((Instance) dataInstance);
        if (votes.length == 0) {
            // e.g. if the model is not trained yet
            return null;
        }

        // Check which loss function was chosen
        int lossIndex = learner.lossFunctionOption.getChosenIndex();
        switch (lossIndex) {
            // 0 => hinge loss (SVM), or 1 => logistic => both are for classification
            case 0: // hinge (binary classification)
            {
                int predictionIndex = moa.core.Utils.maxIndex(votes);
                return this.instancesHeader.classAttribute().value(predictionIndex);
            }
            case 1: // logistic (binary classification)
            {
                int predictionIndex = moa.core.Utils.maxIndex(votes);
                return this.instancesHeader.classAttribute().value(predictionIndex);
            }

            case 2: // squared loss => regression
            {
                // Usually votes has length 1 => the numeric prediction
                double predictedValue = votes[0];
                return predictedValue; // or return Double.valueOf(predictedValue);
            }

            default:
                // If you want to handle unexpected indices gracefully:
                throw new IllegalStateException("SGD lossFunctionOption index " + lossIndex
                        + " not recognized. Expected 0,1,2");
        }
    }



    @Override
    public long size() {
        // Could return a measure of model size; leaving at 0 for now
        return 0;
    }

    @Override
    protected void applyHyperParams() {

        // 2) Configure for regression by using squared loss
        // MOA’s SGD has an option: 0=hinge,1=logistic,2=squared
        // 2 => squared => regression

        if (hyperParams.containsKey("lossFunctionOption")) {
            int option = toInt(hyperParams.get("lossFunctionOption"));
            learner.lossFunctionOption.setChosenIndex(option);
        }

        if (hyperParams.containsKey("learningRate")) {
            int learningRate = toInt(hyperParams.get("learningRate"));
            learner.learningRateOption.setValue(learningRate);
        }

        if (hyperParams.containsKey("lambdaRegularization")) {
            int lambdaRegularization = toInt(hyperParams.get("lambdaRegularization"));
            learner.lambdaRegularizationOption.setValue(lambdaRegularization);
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

    // Additional getters
    public InstancesHeader getInstancesHeader() {
        return this.instancesHeader;
    }

    public int getInstanceCount() {
        return instanceCount;
    }

    public void setLearner(SGD learner) {
        this.learner=learner;
    }

    public SGD getLearner()
    {
        return this.learner;
    }

    public boolean isTrainingComplete() {
        return this.trainingComplete;
    }
    public void setTrainingComplete(boolean trainingComplete) {
        this.trainingComplete = trainingComplete;
    }

    public void setInstanceCount(int instanceCount) {
        this.instanceCount=instanceCount;
    }

    public void setInstancesHeader(InstancesHeader instancesHeader) {
        this.instancesHeader=instancesHeader;
    }
}
