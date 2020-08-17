package com.streever.iot.kafka.spec;

import com.streever.iot.kafka.producer.KafkaProducerConfig;

import java.util.Map;
import java.util.Properties;

public class ProducerSpec {
    private Topic topic;
    private Type type;
    private Properties configs;

    public Topic getTopic() {
        return topic;
    }

    public void setTopic(Topic topic) {
        this.topic = topic;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Properties getConfigs() {
        return configs;
    }

    public void setConfigs(Properties configs) {
        this.configs = configs;
    }

    /**
     * Validate the Spec
     *
     * @return
     */
    public boolean isValid() {
        KafkaProducerConfig[] validConfigs = KafkaProducerConfig.values();
        boolean rtn = true;
        for (Map.Entry<Object, Object> entry : configs.entrySet()) {
            String key = entry.getKey().toString();
            boolean found = false;

            for (KafkaProducerConfig config : validConfigs) {
                if (config.getConfig().equals(key)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                rtn = false;
                System.out.println("Entry: " + key + " is not a valid Producer Property");
            }
        }

        // Check for Minimum Properties

        boolean minFound = true;
        for (KafkaProducerConfig cfg : KafkaProducerConfig.getMinCfgs()) {
//            Object value = configs.contains(cfg.getConfig());
            if (configs.get(cfg.getConfig()) == null) {
                System.out.println("Config: " + cfg.getConfig() + " missing.");
                minFound = false;
            }

        }
        if (!minFound)
            rtn = false;
        
        return rtn;
    }
}
