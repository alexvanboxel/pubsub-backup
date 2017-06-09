package backup.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.util.List;

/**
 * Created by alexvanboxel on 09/06/2017.
 */
public class Config {

    static public class Subscription {
        public String displayName;
        public String topicName;
        public String subscriptionName;
        public String idAttribute;
        public String timestampAttribute;
    }

    public List<Subscription> subscriptions;


    public static Config read(String project) {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        try {
            return mapper.readValue(Config.class.getResourceAsStream(project + ".yaml"), Config.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

