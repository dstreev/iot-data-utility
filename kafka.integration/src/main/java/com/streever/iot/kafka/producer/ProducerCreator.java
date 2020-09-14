package com.streever.iot.kafka.producer;

import com.streever.iot.data.utility.generator.output.kafka.ProducerSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerCreator {

    public static Producer<?, ?> createProducer(ProducerSpec spec) {

        Properties props = spec.getConfigs();

        if (spec.getType().getKey().equalsIgnoreCase("long") &
                spec.getType().getValue().equalsIgnoreCase("string")) {
            props.put(KafkaProducerConfig.KEY_SERIALIZER.getConfig(), LongSerializer.class.getName());
            props.put(KafkaProducerConfig.VALUE_SERIALIZER.getConfig(), StringSerializer.class.getName());

        } else if (spec.getType().getKey().equalsIgnoreCase("string") &
                spec.getType().getValue().equalsIgnoreCase("string")) {
            props.put(KafkaProducerConfig.KEY_SERIALIZER.getConfig(), StringSerializer.class.getName());
            props.put(KafkaProducerConfig.VALUE_SERIALIZER.getConfig(), StringSerializer.class.getName());
        }

        if (props.get(KafkaProducerConfig.KEY_SERIALIZER.getConfig()).equals(LongSerializer.class.getName()) &
                props.get(KafkaProducerConfig.VALUE_SERIALIZER.getConfig()).equals(StringSerializer.class.getName())) {
            return new KafkaProducer<Long, String>(props);
        } else if (props.get(KafkaProducerConfig.KEY_SERIALIZER.getConfig()).equals(StringSerializer.class.getName()) &
                props.get(KafkaProducerConfig.VALUE_SERIALIZER.getConfig()).equals(StringSerializer.class.getName())) {
            return new KafkaProducer<String, String>(props);
        } else {
            throw new UnsupportedOperationException("Only Long/String and String/String Producer types support currently");
        }
   }

}