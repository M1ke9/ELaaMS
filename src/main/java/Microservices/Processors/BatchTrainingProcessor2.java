package Microservices.Processors;


import Structure.DataStructure;
import com.yahoo.labs.samoa.instances.*;
import mlAlgorithms.AMRules.MoaAMRulesRegressorModel;
import mlAlgorithms.Decision_Trees.MoaDecisionTreeModel;
import mlAlgorithms.Decision_Trees.MoaHATModel;
import mlAlgorithms.FIMTDD.MoaFIMTDDModel;
import mlAlgorithms.MLAlgorithm;
import mlAlgorithms.Naive_Bayes.MoaNaiveBayesModel;
import mlAlgorithms.Perceptron.MoaPerceptronModel;
import mlAlgorithms.RandomForest.MoaRandomForestModel;
import mlAlgorithms.RandomForest.MoaRandomForestRegressorModel;
import mlAlgorithms.SGD.MoaSGDModel;
import mlAlgorithms.kNN.MoaKNNModel;
import moa.core.Utils;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

public class BatchTrainingProcessor2 implements Processor<String, DataStructure, Void, Void> {

    private final String modelStoreName;
    private final String algorithmType;
    private final int algorithmId;
    private final String streamID;
    private final String target;
    private final String uniqueModelId;

    private InstancesHeader header;          // ← new field
    private boolean headerBuilt = false;
    private long firstRecordTimeMs = -1;   // set on the very first tuple
    private long processed          = 0;   // total tuples seen so far


    // private final List<String> classValues ;

    // ---------------------- Batching parameters -------------------------
    //private static final int BATCH_SIZE = 10000;  // Flush when buffer hits this size
    //private static final int FLUSH_INTERVAL_SEC = ; // Time-based flush

    private List<DataStructure> buffer;         // In-memory buffer for incoming training data
    private long trainingRecordsProcessed = 0;  // Just for logging

    long trainingStartMillis;

    private boolean started = false;     // Track if we've set trainingStartMillis yet
    private boolean done = false;
    private KeyValueStore<String, MLAlgorithm> modelStore;
    private ProcessorContext<Void, Void> context;

    private String taskType;

    private static final long SAVE_INTERVAL_RECORDS = 50_000;
    private long nextSaveThreshold = SAVE_INTERVAL_RECORDS; // 50_000 initially

    private static final String DEFAULT_MODEL_PATH = "/models/";

    public int  savecount=0;

    Map<String,Object> hyperParams;
    private final long duration;

    enum FieldType { NUMERIC, DATE, TIME, CATEGORICAL }

    private Map<String,FieldType> fieldTypes;

    public BatchTrainingProcessor2(Duration duration,String modelStoreName, String uniqueModelId, String algorithmType,
                                  int algorithmId, String streamID, String target,String taskType,Map<String,Object> hyperParams) {
        this.modelStoreName = modelStoreName;
        this.uniqueModelId = uniqueModelId;
        this.algorithmType = algorithmType;
        this.algorithmId = algorithmId;
        this.streamID = streamID;
        this.target = target;
        // this.classValues=classValues;
        this.taskType=taskType;
        this.hyperParams    = (hyperParams != null) ? hyperParams : new HashMap<>();

        this.duration = duration.toMillis();
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {

        // trainingStartMillis = System.currentTimeMillis();
        this.context = context;
        // Retrieve the KeyValueStore for model storage
        this.modelStore = context.getStateStore(modelStoreName);
        // Initialize the buffer
        //this.buffer = new ArrayList<>(BATCH_SIZE);
        this.buffer = new ArrayList<>();

        context.schedule(
                Duration.ofMillis(duration),
                PunctuationType.WALL_CLOCK_TIME,
                timestamp -> flushBuffer()
        );
    }

    @Override
    public void process(Record<String, DataStructure> record) {

        if (firstRecordTimeMs < 0) {
            firstRecordTimeMs = System.currentTimeMillis();
        }

        // every record bumps the counter
        processed++;


        if (!started) {
            started = true;
            trainingStartMillis = System.currentTimeMillis();
        }


        if (!headerBuilt) {
            this.header = buildHeaderFromFields(record.value());
            headerBuilt = true;
        }


        DataStructure dataInstance = record.value();
        if (dataInstance==null) {
            return;
        }
        // Add to in-memory buffer
        buffer.add(record.value());


        // If we reached the batch size, flush the buffer
        // if (buffer.size() >= BATCH_SIZE) {
        //  flushBuffer();
        // }
    }
//dsds
    /**
     * Flush buffer: retrieve (or create) the model, train on each instance in the batch, then store updated model.
     */
    private void flushBuffer() {


        if (!headerBuilt) return;    // can’t train until header ready

        if (buffer.isEmpty()) {
            return;
        }


        // 1) Get or create the model from the state store
        MLAlgorithm model = modelStore.get(uniqueModelId);
        if (model == null) {
            System.out.println("CREATING MODEL FROM SCRATCH...");
            // Use the first record in the batch to build an InstancesHeader
            DataStructure firstData = buffer.get(0);
            model = createModel(algorithmType, algorithmId, firstData);
            modelStore.put(uniqueModelId, model);


            if (model.getHyperParams() != null && !model.getHyperParams().isEmpty()) {
                System.out.println("Parameters for new " + algorithmType +  " model => " + model.getHyperParams());
            } else {
                System.out.println("No hyperparameters provided => using defaults.");
            }

        }

        // 2) Train on each record in the buffer
        for (DataStructure data : buffer) {

            Instance instance = convertToInstance(data, model);
            if (instance != null) {
                model.train(instance);
                trainingRecordsProcessed++;
            }
        }


        // 5) Clear the buffer
        // buffer.clear();


        if (trainingRecordsProcessed >= 100000 && !done) {


            long now = System.currentTimeMillis();
            long totalseconds =(now - trainingStartMillis)/1000;
            System.out.println("Trained 100k records in " + totalseconds + "seconds"+ "for algorithm: " + algorithmType + ", streamID: " + streamID + ", target: " + target);
            done = true;

            System.out.println("MODEL:"+ model);

        }


        // 3) Store the updated model in the state store
        modelStore.put(uniqueModelId, model);




        // 4) Logging
        if (trainingRecordsProcessed % 10_000 == 0 ) {
            System.out.println("BatchTrainingProcessor processed "
                    + trainingRecordsProcessed + " records total for (algorithm: "
                    + algorithmType + ", streamID: " + streamID + ", target: " + target + ")");
        }

        //   System.out.println("Flushed training batch "+
        // ", total trained so far = " + trainingRecordsProcessed+" for algorithmId " + algorithmType);

        // 5) Clear the buffer
        buffer.clear();


        if (trainingRecordsProcessed >= nextSaveThreshold) {
            saveModelToDisk(model, uniqueModelId);
            nextSaveThreshold += SAVE_INTERVAL_RECORDS;
            savecount++;
            System.out.println("SAVE :" +savecount);
        }





    }

    /**
     * Create a fresh ML model with a new InstancesHeader if none found in the store.
     */
    private MLAlgorithm createModel(String algorithmType,int algorithmId,DataStructure firstData) {

     //   InstancesHeader header = buildHeaderFromFields(firstData);

        if ("Decision-Trees".equalsIgnoreCase(algorithmType) && algorithmId == 1) {
            return new MoaDecisionTreeModel(algorithmId, algorithmType, hyperParams, header);
        }
        else if ("Naive-Bayes".equalsIgnoreCase(algorithmType) && algorithmId == 2) {
            return new MoaNaiveBayesModel(algorithmId, algorithmType, hyperParams, header);
        }
        else if ("kNN".equalsIgnoreCase(algorithmType) && algorithmId == 3) {
            return new MoaKNNModel(algorithmId, algorithmType, hyperParams, header);
        }

        else if ("Random-Forest".equalsIgnoreCase(algorithmType) && algorithmId == 4) {
            return new MoaRandomForestModel(algorithmId, algorithmType, hyperParams, header);
        }
        else if ("Perceptron".equalsIgnoreCase(algorithmType) && algorithmId == 5) {
            return new MoaPerceptronModel(algorithmId, algorithmType, hyperParams, header);
        }

        else if ("HAT".equalsIgnoreCase(algorithmType) && algorithmId == 6) {
            return new MoaHATModel(algorithmId, algorithmType, hyperParams, header);
        }

        else if ("SGD".equalsIgnoreCase(algorithmType) && algorithmId == 7) {
            return new MoaSGDModel(algorithmId, algorithmType, hyperParams, header);
        }


        else if ("FIMT-DD".equalsIgnoreCase(algorithmType) && algorithmId == 8) {
            return new MoaFIMTDDModel(algorithmId, algorithmType, hyperParams, header);

        }

        else if ("AMRules".equalsIgnoreCase(algorithmType) && algorithmId == 9) {
            return new MoaAMRulesRegressorModel(algorithmId, algorithmType, hyperParams, header);
        }


        else if ("Random-Forest-Regressor".equalsIgnoreCase(algorithmType) && algorithmId == 10) {
            return new MoaRandomForestRegressorModel(algorithmId, algorithmType, hyperParams, header);
        }

        throw new IllegalArgumentException("Unsupported algorithm type: " + algorithmType
                + ", ID: " + algorithmId);
    }

    /**
     * Build InstancesHeader from the fields in the DataStructure
     */
    private InstancesHeader buildHeaderFromFields(DataStructure data) {
        Map<String, Object> fields = data.getFields();
        if (fields == null || fields.isEmpty()) {
            System.err.println("No fields in data, cannot build InstancesHeader");
            return null;
        }

        // 1) Figure out each feature’s type once
        List<String> featureNames = new ArrayList<>(fields.keySet());
        featureNames.remove(target);  // don’t infer on the label
        fieldTypes = new HashMap<>();
        for (String feat : featureNames) {
            Object raw = fields.get(feat);
            if (raw instanceof Number) {
                fieldTypes.put(feat, FieldType.NUMERIC);
            } else {
                String s = raw.toString();
                // try date
                try {
                    LocalDate.parse(s, DateTimeFormatter.ofPattern("dd/MM/yyyy"));
                    fieldTypes.put(feat, FieldType.DATE);
                    continue;
                } catch (DateTimeParseException ignored) { }
                // try time
                try {
                    LocalTime.parse(s, DateTimeFormatter.ofPattern("HH:mm:ss"));
                    fieldTypes.put(feat, FieldType.TIME);
                    continue;
                } catch (DateTimeParseException ignored) { }
                // otherwise
                fieldTypes.put(feat, FieldType.CATEGORICAL);
            }
        }

        // 2) Build numeric attributes for each feature
        List<Attribute> attributes = new ArrayList<>();
        for (String feat : featureNames) {
            attributes.add(new Attribute(feat)); // always numeric in the header
        }

        // 3) Add class attribute
        if (taskType.equalsIgnoreCase("classification")) {
            List<String> dummyList = new ArrayList<>();
            if (algorithmId == 5) {
                dummyList = Arrays.asList(
                        "0","1","2","3","4","5","6","7","8","9","10","11","12","13","15"
                );
            }
            System.out.println("Into classification");
            Attribute classAttr = new Attribute(target, dummyList);
            System.out.println("IS CLASS ATR nominal? " + classAttr.isNominal());
            attributes.add(classAttr);
        } else {
            System.out.println("Into regression");
            Attribute classAttr = new Attribute(target);
            System.out.println("IS CLASS ATR nominal? " + classAttr.isNominal());
            attributes.add(classAttr);
        }

        // 4) Package into InstancesHeader
        InstancesHeader header = new InstancesHeader(
                new Instances("Data", attributes, 0)
        );
        header.setClassIndex(attributes.size() - 1);

        System.out.println("TRAINING #attrs: " + header.numAttributes()
                + "  classIndex=" + header.classIndex());

        return header;
    }



    /**
     * Convert DataStructure -> Instance
     */
    private Instance convertToInstance(DataStructure data, MLAlgorithm model) {
        InstancesHeader header = null;

        if (model instanceof MoaDecisionTreeModel) {
            header = ((MoaDecisionTreeModel) model).getInstancesHeader();
        }
        else if (model instanceof MoaNaiveBayesModel) {
            header = ((MoaNaiveBayesModel) model).getInstancesHeader();
        }
        else if (model instanceof MoaKNNModel) {
            header = ((MoaKNNModel) model).getInstancesHeader();
        }

        else if (model instanceof MoaRandomForestModel) {
            header = ((MoaRandomForestModel) model).getInstancesHeader();
        }

        else if (model instanceof MoaPerceptronModel) {
            header = ((MoaPerceptronModel) model).getInstancesHeader();
        }

        else if (model instanceof MoaHATModel) {
            header = ((MoaHATModel) model).getInstancesHeader();
        }

        else if (model instanceof MoaSGDModel) {
            header = ((MoaSGDModel) model).getInstancesHeader();
        }

        else if (model instanceof MoaFIMTDDModel) {
            header = ((MoaFIMTDDModel) model).getInstancesHeader();
        }

        else if (model instanceof MoaAMRulesRegressorModel) {
            header = ((MoaAMRulesRegressorModel) model).getInstancesHeader();
        }


        else if (model instanceof MoaRandomForestRegressorModel) {
            header = ((MoaRandomForestRegressorModel) model).getInstancesHeader();
        }

        if (header == null) {
            System.err.println("Model has no InstancesHeader – cannot build instance");
            return null;
        }

        Map<String, Object> fieldsMap = data.getFields();
        double[] values = new double[header.numAttributes()];
        for (int attrIndex = 0; attrIndex < header.numAttributes(); attrIndex++) {
            Attribute attr = header.attribute(attrIndex);
            String attrName = attr.name();
            if (attrIndex == header.classIndex()) {
                // It's the target

                if(attr.isNominal()) {
                    Object rawLabel = fieldsMap.get(target);
                    values[attrIndex] = encodeNominalValue(attr, rawLabel);
                }
                else
                {
                   // Object rawVal = fieldsMap.get(target);
                   // values[attrIndex] = parseNumericOrMissing(rawVal);
                    values[attrIndex] = parseByType(fieldsMap.get(target), FieldType.NUMERIC, attr);
                }
            } else {
                // It's a feature
              //  Object rawVal = fieldsMap.get(attrName);
               // values[attrIndex] = parseNumericOrMissing(rawVal);
                FieldType t = fieldTypes.getOrDefault(attrName, FieldType.NUMERIC);
                values[attrIndex] = parseByType(fieldsMap.get(attrName), t, attr);

            }
        }

        Instance inst = new DenseInstance(1.0, values);
        inst.setDataset(header);
        return inst;
    }

    double parseByType(Object raw, FieldType t, Attribute attr) {
        if (raw==null) return Utils.missingValue();
        String s = raw.toString();
        try {
            switch(t) {
                case NUMERIC:  return Double.parseDouble(s);
                case DATE:
                    LocalDate d = LocalDate.parse(s, DateTimeFormatter.ofPattern("dd/MM/yyyy"));
                    return d.toEpochDay();
                case TIME:
                    LocalTime tm = LocalTime.parse(s, DateTimeFormatter.ofPattern("HH:mm:ss"));
                    return tm.toSecondOfDay();
                case CATEGORICAL:
                    return attr.indexOfValue(s);
            }
        } catch (Exception e) {
            System.out.println("MISSING VALUE" + raw + " fieldtype:" +t + " attr"+ attr);
            return Utils.missingValue();
        }
        return Utils.missingValue();
    }


    private double encodeNominalValue(Attribute classAttr, Object rawLabel) {
        if (rawLabel == null) {
            return Utils.missingValue();
        }
        String labelStr = rawLabel.toString();
        int idx = classAttr.indexOfValue(labelStr);
        if (idx == -1) {
            // Not recognized
            System.out.println("########### MISSING VALUE #############");
            return Utils.missingValue();
        }
        return idx;
    }


    private void saveModelToDisk(MLAlgorithm model, String modelId) {
        // Example: write to /tmp/model-checkpoints/<modelId>.bin


        String path = "C:/dataset/Stored/" + modelId + ".bin";

        //  String basePath = System.getenv().getOrDefault("MODEL_STORAGE_PATH", DEFAULT_MODEL_PATH);
        //  String path = basePath + modelId + ".bin";

        try (FileOutputStream fos = new FileOutputStream(path);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(model);
            oos.flush();
            System.out.println("Saved model to disk at " + path);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Failed to save model to disk: " + e.getMessage());
        }
    }

    @Override
    public void close() {

        long elapsedMs = System.currentTimeMillis() - firstRecordTimeMs;
        if (elapsedMs > 0) {
            double tps = processed * 1000.0 / elapsedMs;
            System.out.printf("[THROUGHPUT] %s → %,d tuples in %.2fs  (%.1f tuples/sec)%n",
                    uniqueModelId, processed, elapsedMs/1000.0, tps);
        }
        // Ensure we flush any remaining records at shutdown
        flushBuffer();
    }
}
