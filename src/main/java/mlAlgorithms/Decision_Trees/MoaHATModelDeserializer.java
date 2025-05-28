package mlAlgorithms.Decision_Trees;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import moa.classifiers.trees.HoeffdingAdaptiveTree;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;



public class MoaHATModelDeserializer implements Deserializer<MoaHATModel> {

    @Override
    public MoaHATModel deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0)
            return null;
        try (ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
             DataInputStream dataIn = new DataInputStream(byteStream)) {

            // Read the algorithmId
            int algorithmId = dataIn.readInt();
            //System.out.println("Deserialized MoaDecisionTreeModel algorithmId: " + algorithmId);

            // Read the algorithmType
            String algorithmType = dataIn.readUTF();

            //String modelID = dataIn.readUTF();
            // Read the algorithmParameters
            // String algorithmParameters = dataIn.readUTF();

            String hpJson = dataIn.readUTF();  // the hyperParams JSON

            Map<String,Object> hyperParams = new ObjectMapper().readValue(hpJson, new TypeReference<Map<String,Object>>(){});


            // Deserialize the rest of the object
            try (ObjectInputStream objectIn = new ObjectInputStream(dataIn)) {
                HoeffdingAdaptiveTree classifier = (HoeffdingAdaptiveTree) objectIn.readObject();
                int instanceCount = objectIn.readInt();
                InstancesHeader instancesHeader = (InstancesHeader) objectIn.readObject();
                boolean trainingComplete = objectIn.readBoolean();

                // Reconstruct the MoaDecisionTreeModel object
                MoaHATModel model = new MoaHATModel(algorithmId,algorithmType,hyperParams,instancesHeader);
               // model.setAlgorithmId(algorithmId);
              //  model.setAlgorithmType(algorithmType);
                // model.setModelID(modelID);
                // model.setAlgorithmParameters(Collections.singletonList(algorithmParameters));
                model.setClassifier(classifier);
                model.setInstanceCount(instanceCount);
               // model.setInstancesHeader(instancesHeader);
                model.setTrainingComplete(trainingComplete);
               // model.setHyperParams(hyperParams); // => re-applies them if needed


                return model;
            }

        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Error deserializing MoaDecisionTreeModel", e);
        }
    }
}
