package helperClasses;

import Configuration.EnvironmentConfiguration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class CreateTopic {

    //private static final String  BOOTSTRAP_SERVERS = "broker:9092";

    private static final String BOOTSTRAP_SERVERS = EnvironmentConfiguration.getBootstrapServers();
    public CreateTopic(){}




    public  void createTopicsForStreamIdAndTargetIfNotExists(String TopicName,String aggregatorKey,String target) {


       String topicName = TopicName+"-"+aggregatorKey+"-"+target;

        try (AdminClient admin = AdminClient.create(createAdminProps())) {
            Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> topics = new ArrayList<>();
            if (!existing.contains(topicName)) {
                topics.add(new NewTopic(topicName, 1, (short)EnvironmentConfiguration.giveTheReplicationFactor()));
            }

            if (!topics.isEmpty()) {
                admin.createTopics(topics).all().get();
                System.out.printf("Created Ensemble topic for key =%s => %s %n",
                        aggregatorKey,topicName);
            }
        } catch (ExecutionException e) {
            if (e.getCause() != null
                    && e.getCause().getClass().getSimpleName().equals("TopicExistsException")) {
                System.out.println("Ensemble Topic already exist => ignoring. key =" + aggregatorKey);
            } else {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private Properties createAdminProps() {
        Properties properties = new Properties();
        //props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put( AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return properties;
    }

    }


