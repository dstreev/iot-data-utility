package com.streever.iot.data.utility.generator.output.kafka;

public enum KafkaProducerConfig {

    KEY_SERIALIZER("key.serializer"),
    VALUE_SERIALIZER("value.serializer"),
    ACKS("acks"),
    BOOTSTRAP_SERVERS("bootstrap.servers"),
    BUFFER_MEMORY("buffer.memory"),
    COMPRESSION_TYPE("compression.type"),
    RETRIES("retries"),
    SSL_KEY_PASSWORD("ssl.key.password"),
    SSL_KEYSTORE_LOCATION("ssl.keystore.location"),
    SSL_KEYSTORE_PASSWORD("ssl.keystore.password"),
    SSL_TRUSTSTORE_LOCATION("ssl.truststore.location"),
    SSL_TRUSTSTORE_PASSWORD("ssl.truststore.password"),
    BATCH_SIZE("batch.size"),
    CLIENT_ID("client.id"),
    CONNECTIONS_MAX_IDLE_MS("connections.max.idle.ms"),
    LINGER_MS("linger.ms"),
    MAX_BLOCK_MS("max.block.ms"),
    MAX_REQUEST_SIZE("max.request.size"),
    PARTITIONER_CLASS("partitioner.class"),
    RECEIVE_BUFFER_BYTES("receive.buffer.bytes"),
    REQUEST_TIMEOUT_MS("request.timeout.ms"),
    SASL_JAAS_CONFIG("sasl.jaas.config"),
    SASL_KERBEROS_SERVICE_NAME("sasl.kerberos.service.name"),
    SASL_MECHANISM("sasl.mechanism"),
    SECURITY_PROTOCOL("security.protocol"),
    SEND_BUFFER_BYTES("send.buffer.bytes"),
    SSL_ENABLED_PROTOCOLS("ssl.enabled.protocols"),
    SSL_KEYSTORE_TYPE("ssl.keystore.type"),
    SSL_PROTOCOL("ssl.protocol"),
    SSL_PROVIDER("ssl.provider"),
    SSL_TRUSTSTORE_TYPE("ssl.truststore.type"),
    ENABLE_IDEMPOTENCE("enable.idempotence"),
    INTERCEPTOR_CLASS("interceptor.classes"),
    MAX_IN_FLIGHT_REQUEST_PER_CONNECTION("max.in.flight.requests.per.connection"),
    METADATA_MAX_AGE_MS("metadata.max.age.ms"),
    METRIC_REPORTERS("metric.reporters"),
    METRICS_NUM_SAMPLES("metrics.num.samples"),
    METRICS_RECORDING_LEVEL("metrics.recording.level"),
    METRICS_SAMPLE_WINDOW_MS("metrics.sample.window.ms"),
    RECONNECT_BACKOFF_MAX_MS("reconnect.backoff.max.ms"),
    RECONNECT_BACKOFF_MS("reconnect.backoff.ms"),
    RETRY_BACKOFF_MS("retry.backoff.ms"),
    SASL_KERBEROS_KINIT_CMD("sasl.kerberos.kinit.cmd"),
    SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN("sasl.kerberos.min.time.before.relogin"),
    SASL_KERBEROS_TICKET_RENEW_JITTER("sasl.kerberos.ticket.renew.jitter"),
    SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR("sasl.kerberos.ticket.renew.window.factor"),
    SSL_CIPHER_SUITES("ssl.cipher.suites"),
    SSL_ENDPOINT_IDENTIFICATION_ALGORITHM("ssl.endpoint.identification.algorithm"),
    SSL_KEYMANAGER_ALGORITHM("ssl.keymanager.algorithm"),
    SSL_SECURE_RANDOM_IMPLEMENTATION("ssl.secure.random.implementation"),
    SSL_TRUSTMANAGER_ALGORITHM("ssl.trustmanager.algorithm"),
    TRANSACTION_TIMEOUT_MS("transaction.timeout.ms"),
    TRANSACTIONAL_ID("transactional.id");
    

    private String config;

    KafkaProducerConfig(String config) {
        this.config = config;
    }

    public String getConfig() {
        return config;
    }

    public static KafkaProducerConfig[] getMinCfgs() {
        KafkaProducerConfig[] rtn = { CLIENT_ID, BOOTSTRAP_SERVERS, ACKS };
        return rtn;
    }
}
