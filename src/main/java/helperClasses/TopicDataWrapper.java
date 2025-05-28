package helperClasses;

import Structure.DataStructure;

public class TopicDataWrapper {
    private final String topicName;
    private final DataStructure data;

    public TopicDataWrapper(String topicName, DataStructure data) {
        this.topicName = topicName;
        this.data = data;
    }

    public String getTopicName() {
        return topicName;
    }

    public DataStructure getData() {
        return data;
    }
}