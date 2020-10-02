package com.streever.iot.data.utility.generator.output.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.streever.iot.data.utility.generator.Schema;
import com.streever.iot.data.utility.generator.output.OutputBase;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

@JsonIgnoreProperties({"producer"})
public class KafkaOutput extends OutputBase {
    private Topic topic = new Topic();
    private RecordType recordType = new RecordType();
    private Properties configs = new Properties();
    private Producer producer = null;

    public Topic getTopic() {
        return topic;
    }

    public void setTopic(Topic topic) {
        this.topic = topic;
    }

    public RecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(RecordType recordType) {
        this.recordType = recordType;
    }

    public Properties getConfigs() {
        return configs;
    }

    public void setConfigs(Properties configs) {
        this.configs = configs;
    }

    @Override
    protected void writeLine(String line) throws IOException {
        // TODO:
//        ProducerRecord<String, String> rec = new ProducerRecord<String, String>();
//        producer.send(rec);
    }

    @Override
    public void link(Schema record) {
        // TODO:
    }

    @Override
    public boolean open(String prefix) throws IOException {
        producer = ProducerCreator.createProducer(this);
        setOpen(true);
        return true;
    }

    @Override
    public boolean close() throws IOException {
        if (isOpen()) {
            producer.close();;
        }
        return true;
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

    @Override
    public Object clone() throws CloneNotSupportedException {
        KafkaOutput clone = (KafkaOutput)super.clone();
        clone.setTopic((Topic)this.getTopic().clone());
        clone.setConfigs((Properties)this.getConfigs().clone());
        clone.setRecordType((RecordType)this.getRecordType().clone());
        return clone;
    }
}
