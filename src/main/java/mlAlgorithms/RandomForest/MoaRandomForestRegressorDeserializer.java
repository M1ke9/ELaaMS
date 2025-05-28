package mlAlgorithms.RandomForest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import moa.classifiers.meta.AdaptiveRandomForestRegressor;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;




public class MoaRandomForestRegressorDeserializer implements Deserializer<MoaRandomForestRegressorModel> {

    @Override
    public MoaRandomForestRegressorModel deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0)
            return null;
        try (ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
             DataInputStream dataIn = new DataInputStream(byteStream)) {

            // Read the algorithmId
            int algorithmId = dataIn.readInt();
            //  System.out.println("Deserialized MoaNaiveBayesModel algorithmId: " + algorithmId);

            // Read the algorithmType
            String algorithmType = dataIn.readUTF();

            //  String modelID = dataIn.readUTF();

            // Read the algorithmParameters
            //String algorithmParameters = dataIn.readUTF();

            String hpJson = dataIn.readUTF();  // the hyperParams JSON

            Map<String,Object> hyperParams = new ObjectMapper().readValue(hpJson, new TypeReference<Map<String,Object>>(){});


            // Deserialize the rest of the object
            try (ObjectInputStream objectIn = new ObjectInputStream(dataIn)) {
                AdaptiveRandomForestRegressor regressor = (AdaptiveRandomForestRegressor) objectIn.readObject();
                int instanceCount = objectIn.readInt();
                InstancesHeader instancesHeader = (InstancesHeader) objectIn.readObject();
                boolean trainingComplete = objectIn.readBoolean();

                // Reconstruct the MoaDecisionTreeModel object
                MoaRandomForestRegressorModel model = new MoaRandomForestRegressorModel(algorithmId,algorithmType,hyperParams,instancesHeader);
                //model.setAlgorithmId(algorithmId);
              //  model.setAlgorithmType(algorithmType);
                //  model.setModelID(modelID);
                // model.setAlgorithmParameters(Collections.singletonList(algorithmParameters));
                model.setRegressor(regressor);
                model.setInstanceCount(instanceCount);
              //  model.setInstancesHeader(instancesHeader);
                model.setTrainingComplete(trainingComplete);
             //   model.setHyperParams(hyperParams);



                return model;
            }

        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Error deserializing MoaAdaptiveRandomForestModel", e);
        }
    }
}

