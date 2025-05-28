package mlAlgorithms.FIMTDD;

import Serdes.FIMTDD.MoaFIMTDDModelSerde;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import mlAlgorithms.MLAlgorithm;
import moa.classifiers.trees.FIMTDD;
import moa.classifiers.trees.SafeFIMTDD;
import org.apache.kafka.common.serialization.Serde;

//import moa.classifiers.meta.


import java.io.Serializable;
import java.util.Map;

public class MoaFIMTDDModel extends MLAlgorithm implements Serializable {
    //   @Serial
    // private static final long serialVersionUID = 1L;
    private FIMTDD regressor;
    private int instanceCount;
    //private InstancesHeader instancesHeader;
    private boolean trainingComplete = false;


    public MoaFIMTDDModel(int algorithmId, String algorithmType , Map<String,Object> hyperParams, InstancesHeader instancesHeader) {
        super(algorithmId, algorithmType, hyperParams,instancesHeader);

        this.regressor = new SafeFIMTDD();
        applyHyperParams();
        //this.instancesHeader = instancesHeader;
        this.regressor.setModelContext(this.instancesHeader);
        this.regressor.prepareForUse();
        this.instanceCount = 0;
    }

    public MoaFIMTDDModel() {
        super();
    }

    @Override
    public Serde<MoaFIMTDDModel> serde() {
        // If you have a specialized SerDe for Naive Bayes, use that. For now, using the decision tree one as placeholder.
        return new MoaFIMTDDModelSerde();
    }

    /*
    @Override
    public void train(Object dataInstance) {
        if (dataInstance instanceof Instance) {
            fimtdd.trainOnInstance((Instance) dataInstance);
            instanceCount++;
        } else {
            throw new IllegalArgumentException("Data instance must be of type Instance.");
        }
    }

     */

    @Override
    public void train(Object dataInstance) {
        if (!(dataInstance instanceof Instance))
            throw new IllegalArgumentException("Data instance must be Instance");

        try {
            regressor.trainOnInstance((Instance) dataInstance);
            instanceCount++;
        } catch (IndexOutOfBoundsException ex) {
            // branchForInstance returned -1 ⇒ SKIP this record
            // (happens on missing or out‑of‑range attribute values)
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
            return votes[0];   // numeric regression
        } catch (NullPointerException npe) {
            // FIMT‑DD didn’t have a child for this instance → skip this record
            return null;
        }
    }



    @Override
    public long size() {
        return 0;
    }



        @Override
        protected void applyHyperParams() {
            if (hyperParams.containsKey("gracePeriod")) {
                int grace = toInt(hyperParams.get("gracePeriod"));
                regressor.gracePeriodOption.setValue(grace);
            }
            if (hyperParams.containsKey("splitConfidence")) {
                double splitConf = toDouble(hyperParams.get("splitConfidence"));
                regressor.splitConfidenceOption.setValue(splitConf);
            }
            if (hyperParams.containsKey("tieThreshold")) {
                double tieTh = toDouble(hyperParams.get("tieThreshold"));
                regressor.tieThresholdOption.setValue(tieTh);
            }

            if (hyperParams.containsKey("learningRatio")) {
                double learningRatio = toDouble(hyperParams.get("learningRatio"));
                regressor.tieThresholdOption.setValue(learningRatio);
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

    public FIMTDD getRegressor() {
        return this.regressor;
    }

    public void setRegressor(FIMTDD regressor) {
        this.regressor=regressor;
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