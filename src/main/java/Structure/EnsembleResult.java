package Structure;

import java.util.List;

public class EnsembleResult {
    private String recordId;
    private String finalPrediction;
    private List<PartialPrediction> partialPredictions;

    public EnsembleResult() {
        // Empty constructor needed for Jackson
    }

    public EnsembleResult(String recordId, String finalPrediction, List<PartialPrediction> partialPredictions) {
        this.recordId = recordId;
        this.finalPrediction = finalPrediction;
        this.partialPredictions = partialPredictions;
    }

    // Getters & setters
    public String getRecordId() {
        return recordId;
    }
    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    public String getFinalPrediction() {
        return finalPrediction;
    }
    public void setFinalPrediction(String finalPrediction) {
        this.finalPrediction = finalPrediction;
    }

    public List<PartialPrediction> getPartialPredictions() {
        return partialPredictions;
    }
    public void setPartialPredictions(List<PartialPrediction> partialPredictions) {
        this.partialPredictions = partialPredictions;
    }

    @Override
    public String toString() {
        return "EnsembleResult{" +
                "recordId='" + recordId + '\'' +
                ", finalLabel='" + finalPrediction + '\'' +
                ", partialPredictions=" + partialPredictions +
                '}';
    }
}
