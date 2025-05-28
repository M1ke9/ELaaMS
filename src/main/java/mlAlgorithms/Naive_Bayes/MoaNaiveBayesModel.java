package mlAlgorithms.Naive_Bayes;

import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import moa.classifiers.bayes.NaiveBayes;
import moa.core.Utils;
import org.apache.kafka.common.serialization.Serde;
import mlAlgorithms.MLAlgorithm;
import Serdes.Naive_Bayes.MoaNaiveBayesModelSerde;

//import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

public class MoaNaiveBayesModel extends MLAlgorithm implements Serializable {
   // @Serial
   // private static final long serialVersionUID = 1L;
    private NaiveBayes classifier;
    private int instanceCount;
   // private InstancesHeader instancesHeader;
    private boolean trainingComplete = false;


    public MoaNaiveBayesModel(int algorithmId, String algorithmType , Map<String,Object> hyperParams, InstancesHeader instancesHeader) {
        super(algorithmId, algorithmType, hyperParams,instancesHeader);
        this.classifier = new NaiveBayes();

        applyHyperParams();
        //this.instancesHeader = instancesHeader;
        this.classifier.setModelContext(this.instancesHeader);
        this.classifier.prepareForUse();
        this.instanceCount = 0;
    }

    public MoaNaiveBayesModel() {
        // Default constructor needed for deserialization or lazy initialization
        super();
    }

    @Override
    public Serde<MoaNaiveBayesModel> serde() {
        return new MoaNaiveBayesModelSerde();
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
        // Naive Bayes models store frequency counts internally.
        // There's no straightforward "size" measurement like tree nodes.
        // Return 0 or implement a custom metric if needed.
        return 0;
    }

    @Override
    protected void applyHyperParams() {
        if(hyperParams.containsKey("RandomSeed"))
        {
            int RandomSeed = toInt(hyperParams.get("RandomSeed"));
            this.classifier.setRandomSeed(RandomSeed);
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

    public NaiveBayes getClassifier() {
        return this.classifier;
    }

    public void setClassifier(NaiveBayes classifier) {
        this.classifier = classifier;
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