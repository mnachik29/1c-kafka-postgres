package ru.integration.sink.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.integration.sink.Config;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import static net.logstash.logback.argument.StructuredArguments.kv;

/**
 * Отправляет необрабатываемые сообщения в dead-letter topic (например, 1c.counterparties.dlq)
 * с исходным ключом/значением и заголовками о причине ошибки.
 */
public class DlqPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);

    private final Config config;
    private final KafkaProducer<String, String> producer;
    private final List<Future<?>> pending = new ArrayList<>();

    public DlqPublisher(Config config) {
        this.config = config;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrap());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "consumer-service-dlq");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        this.producer = new KafkaProducer<>(props);
    }

    public void send(ConsumerRecord<String, String> source, String reason) {
        String topic = config.dlqTopic(source.topic());
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, source.key(), source.value());
        record.headers().add("dlq.reason", bytes(reason));
        record.headers().add("dlq.source.topic", bytes(source.topic()));
        record.headers().add("dlq.source.partition", bytes(String.valueOf(source.partition())));
        record.headers().add("dlq.source.offset", bytes(String.valueOf(source.offset())));
        record.headers().add("dlq.failed_at", bytes(Instant.now().toString()));
        log.error("Message sent to DLQ", kv("dlq_topic", topic), kv("source_topic", source.topic()),
                kv("partition", source.partition()), kv("offset", source.offset()), kv("key", source.key()),
                kv("reason", reason));
        pending.add(producer.send(record));
    }

    /** Ждёт подтверждения DLQ-сообщений; без этого нельзя коммитить offset исходного топика. */
    public void flush() throws Exception {
        producer.flush();
        try {
            for (Future<?> f : pending) {
                f.get();
            }
        } finally {
            pending.clear();
        }
    }

    private static byte[] bytes(String s) {
        return (s == null ? "" : s).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        producer.close();
    }
}
