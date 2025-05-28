package Serdes;

import Serdes.AMRulesRegressor.MoaAMRulesRegressorModelSerde;
import Serdes.Decision_Trees.MoaHATModelSerde;
import Serdes.FIMTDD.MoaFIMTDDModelSerde;
import Serdes.Perceptron.MoaPerceptronModelSerde;
import Serdes.RandomForest.MoaAdaptiveRandomForestRegressorSerde;
import Serdes.RandomForest.MoaAdaptiveRandomForestSerde;
import Serdes.SGD.MoaSGDModelSerde;
import mlAlgorithms.AMRules.MoaAMRulesRegressorModel;
import mlAlgorithms.Decision_Trees.MoaHATModel;
import mlAlgorithms.FIMTDD.MoaFIMTDDModel;
import mlAlgorithms.MLAlgorithm;
import mlAlgorithms.Decision_Trees.MoaDecisionTreeModel;
import mlAlgorithms.Naive_Bayes.MoaNaiveBayesModel;
import mlAlgorithms.Perceptron.MoaPerceptronModel;
import mlAlgorithms.RandomForest.MoaRandomForestModel;
import mlAlgorithms.RandomForest.MoaRandomForestRegressorModel;
import mlAlgorithms.SGD.MoaSGDModel;
import mlAlgorithms.kNN.MoaKNNModel;
import Serdes.Decision_Trees.MoaDecisionTreeModelSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import Serdes.Naive_Bayes.MoaNaiveBayesModelSerde;
import Serdes.kNN.MoaKNNModelSerde;

import java.io.*;

public class MLAlgorithmSerde implements Serde<MLAlgorithm> {

    private final MoaDecisionTreeModelSerde decisionTreeModelSerde = new MoaDecisionTreeModelSerde();
    private final MoaNaiveBayesModelSerde naiveBayesModelSerde =new MoaNaiveBayesModelSerde();
    private final MoaKNNModelSerde knnModelSerde = new MoaKNNModelSerde();

    private final MoaPerceptronModelSerde perceptronModelSerde = new MoaPerceptronModelSerde();

    private final MoaAdaptiveRandomForestSerde randomForestSerde = new MoaAdaptiveRandomForestSerde();

    private final MoaHATModelSerde  hatModelSerde = new MoaHATModelSerde();
    private final MoaSGDModelSerde sgdModelSerde = new MoaSGDModelSerde();

    private final MoaFIMTDDModelSerde fimtddModelSerde = new MoaFIMTDDModelSerde();

    private final MoaAMRulesRegressorModelSerde amRulesRegressorModelSerde = new MoaAMRulesRegressorModelSerde();

  private final MoaAdaptiveRandomForestRegressorSerde randomForestRegressorSerde = new MoaAdaptiveRandomForestRegressorSerde();


    // Instantiate other Serde instances for different MLAlgorithm subclasses

    @Override
    public Serializer<MLAlgorithm> serializer() {
        return (topic, data) -> {
            if (data == null)
                return null;

            // Directly use the specific serializer
            if (data instanceof MoaDecisionTreeModel) {
                return decisionTreeModelSerde.serializer().serialize(topic, (MoaDecisionTreeModel) data);
            }
            else if (data instanceof MoaNaiveBayesModel) {

                return naiveBayesModelSerde.serializer().serialize(topic,(MoaNaiveBayesModel) data);
            }
            else if (data instanceof MoaKNNModel) {

                return knnModelSerde.serializer().serialize(topic, (MoaKNNModel) data);
            }

            else if (data instanceof MoaRandomForestModel) {

                return randomForestSerde.serializer().serialize(topic, (MoaRandomForestModel) data);
            }
            else if (data instanceof MoaPerceptronModel) {

                return perceptronModelSerde.serializer().serialize(topic, (MoaPerceptronModel) data);
            }

            else if(data instanceof MoaSGDModel){

                return sgdModelSerde.serializer().serialize(topic,(MoaSGDModel) data);
            }

            else if(data instanceof MoaFIMTDDModel){

                return fimtddModelSerde.serializer().serialize(topic,(MoaFIMTDDModel) data);

            }

            else if(data instanceof  MoaAMRulesRegressorModel)
            {
                return amRulesRegressorModelSerde.serializer().serialize(topic,(MoaAMRulesRegressorModel) data);
            }

            else if(data instanceof  MoaHATModel)
            {
                return  hatModelSerde.serializer().serialize(topic,(MoaHATModel) data);
            }

            else if( data instanceof MoaRandomForestRegressorModel)
            {
                return randomForestRegressorSerde.serializer().serialize(topic,(MoaRandomForestRegressorModel) data);
            }

            else {
                throw new IllegalArgumentException("Unknown MLAlgorithm subclass");
            }
        };
    }


    @Override
    public Deserializer<MLAlgorithm> deserializer() {
        return (topic, data) -> {
            if (data == null || data.length == 0)
                return null;

            // Read the algorithmId to determine which deserializer to use
            int algorithmId;
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
                 DataInputStream dataIn = new DataInputStream(byteStream)) {

                algorithmId = dataIn.readInt();
               // System.out.println("ALGORITHMID:" + algorithmId);

                // Reset the stream to include the algorithmId for the specific deserializer
                dataIn.close();
                byteStream.reset();
/*
                return switch (algorithmId) {
                    case 1 -> decisionTreeModelSerde.deserializer().deserialize(topic, data);

                    case 2 -> naiveBayesModelSerde.deserializer().deserialize(topic, data);

                    case 3 -> knnModelSerde.deserializer().deserialize(topic, data);

                    default -> throw new IllegalArgumentException("Unknown algorithmId: " + algorithmId);
                };

 */

                switch (algorithmId) {

                    case 1:
                        return decisionTreeModelSerde.deserializer().deserialize(topic, data);

                    case 2:
                        return naiveBayesModelSerde.deserializer().deserialize(topic, data);

                    case 3:
                        return knnModelSerde.deserializer().deserialize(topic, data);

                    case 4:
                        return randomForestSerde.deserializer().deserialize(topic,data);

                    case 5:
                        return perceptronModelSerde.deserializer().deserialize(topic,data);

                    case 6:
                        return hatModelSerde.deserializer().deserialize(topic,data);
                    case 7:
                         return sgdModelSerde.deserializer().deserialize(topic,data);

                    case 8:
                        return fimtddModelSerde.deserializer().deserialize(topic,data);

                    case 9:
                        return amRulesRegressorModelSerde.deserializer().deserialize(topic,data);

                    case 10:
                        return randomForestRegressorSerde.deserializer().deserialize(topic,data);

                    default:
                        throw new IllegalArgumentException("Unknown algorithmId: " + algorithmId);
                }

            } catch (IOException e) {
                throw new RuntimeException("Error deserializing MLAlgorithm", e);
            }
        };
    }
}




