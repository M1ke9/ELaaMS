package mlAlgorithms.Decision_Trees;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;


public class MoaHATModelSerializer implements Serializer<MoaHATModel> {

    @Override
    public byte[] serialize(String topic, MoaHATModel data) {
        if (data == null)
            return null;
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             DataOutputStream dataOut = new DataOutputStream(byteStream)) {

            // Write the algorithmId
            dataOut.writeInt(data.getAlgorithmId());

            // Write the algorithmType
            dataOut.writeUTF(data.getAlgorithmType());

            // dataOut.writeUTF(data.getModelID());

            // Write the algorithmParameters
            // for ( int i=0 ; i< data.getAlgorithmParameters().size(); i++)
            // dataOut.writeUTF(data.getAlgorithmParameters().get(i));
            Map<String,Object> hp = data.getHyperParams();
            String hpJson = new ObjectMapper().writeValueAsString(hp);
            dataOut.writeUTF(hpJson);



            // Serialize the rest of the object
            try (ObjectOutputStream objectOut = new ObjectOutputStream(dataOut)) {
                objectOut.writeObject(data.getClassifier());
                objectOut.writeInt(data.getInstanceCount());
                objectOut.writeObject(data.getInstancesHeader());
                objectOut.writeBoolean(data.isTrainingComplete());
                objectOut.flush();
            }

            dataOut.flush();
            return byteStream.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException("Error serializing MoaDecisionTreeModel", e);
        }
    }
}
