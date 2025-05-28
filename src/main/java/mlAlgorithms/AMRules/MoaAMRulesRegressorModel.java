package mlAlgorithms.AMRules;

import Serdes.AMRulesRegressor.MoaAMRulesRegressorModelSerde;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import moa.classifiers.rules.AMRulesRegressor;
import org.apache.kafka.common.serialization.Serde;
import mlAlgorithms.MLAlgorithm;


import java.io.Serializable;
import java.util.Map;

public class MoaAMRulesRegressorModel extends MLAlgorithm implements Serializable {

    private AMRulesRegressor regressor;
   // private InstancesHeader instancesHeader;
    private int instanceCount;

    private boolean trainingComplete = false;
    public MoaAMRulesRegressorModel(int algoId, String algoType, Map<String,Object> hyperParams, InstancesHeader instancesHeader) {
        super(algoId, algoType, hyperParams,instancesHeader);
       // this.instancesHeader = header;
        this.regressor = new AMRulesRegressor();
        applyHyperParams();
        // Possibly configure further
        this.regressor.setModelContext(this.instancesHeader);
        this.regressor.prepareForUse();
    }

    public MoaAMRulesRegressorModel() {
        super();
    }

    @Override
    public Serde<MoaAMRulesRegressorModel> serde() {
        // implement or placeholder
        return new MoaAMRulesRegressorModelSerde();
    }

    @Override
    public void train(Object dataInstance) {
        if (dataInstance instanceof Instance) {
            this.regressor.trainOnInstance((Instance)dataInstance);
            instanceCount++;
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

    private int toInt(Object val) {
        if (val instanceof Number) return ((Number)val).intValue();
        return Integer.parseInt(val.toString());
    }
    private double toDouble(Object val) {
        if (val instanceof Number) return ((Number)val).doubleValue();
        return Double.parseDouble(val.toString());
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    protected void applyHyperParams() {
        if (hyperParams.containsKey("gracePeriod")) {
            int gracePeriod = toInt(hyperParams.get("gracePeriod"));
            this.regressor.gracePeriodOption.setValue(gracePeriod);
        }

        if (hyperParams.containsKey("splitConfidence")) {
            double splitConf = toDouble(hyperParams.get("splitConfidence"));
            regressor.splitConfidenceOption.setValue(splitConf);
        }
        if (hyperParams.containsKey("tieThreshold")) {
            double tieTh = toDouble(hyperParams.get("tieThreshold"));
            regressor.tieThresholdOption.setValue(tieTh);
        }


    }


    public int getInstanceCount() {
        return instanceCount;
    }


    public void setRegressor(AMRulesRegressor regressor)
    {
        this.regressor=regressor;
    }

    public AMRulesRegressor getRegressor(){
        return this.regressor;
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
/*
    public void setInstancesHeader(InstancesHeader instancesHeader) {
        this.instancesHeader=instancesHeader;
    }

 */
}
