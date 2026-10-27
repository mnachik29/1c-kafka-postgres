package ru.integration.onec.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.integration.onec.Config;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static net.logstash.logback.argument.StructuredArguments.kv;

/**
 * Публикует события в Kafka. Ключ сообщения — id объекта 1С (Ref_Key), поэтому все версии
 * одного объекта попадают в одну партицию и обрабатываются по порядку.
 */
public class EventPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;
    private final AtomicReference<Exception> firstError = new AtomicReference<>();

    public EventPublisher(Config config, ObjectMapper mapper) {
        this.mapper = mapper;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrap());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "integration-service");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Формирует конверт события и отправляет асинхронно. Ошибки доставки запоминаются
     * и пробрасываются из {@link #flush()}, чтобы не сдвигать метку синхронизации при сбое.
     */
    public void publish(String topic, String eventType, ObjectNode payload) throws Exception {
        String id = payload.get("id").asText();
        ObjectNode event = mapper.createObjectNode();
        event.put("event_id", eventId(eventType, payload).toString());
        event.put("event_type", eventType);
        event.put("source", "1c");
        event.put("occurred_at", Instant.now().toString());
        event.set("payload", payload);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, id, mapper.writeValueAsString(event));
        record.headers().add("event_type", eventType.getBytes(StandardCharsets.UTF_8));
        record.headers().add("source", "1c".getBytes(StandardCharsets.UTF_8));

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                firstError.compareAndSet(null, exception);
                log.error("Kafka send failed", kv("topic", topic), kv("key", id), kv("error", exception.toString()));
            }
        });
    }

    /** Дожидается подтверждения всех отправленных сообщений. */
    public void flush() throws Exception {
        producer.flush();
        Exception error = firstError.getAndSet(null);
        if (error != null) {
            throw new IllegalStateException("Some events were not delivered to Kafka: " + error, error);
        }
    }

    /**
     * Детерминированный event_id: одна и та же версия объекта 1С (DataVersion) всегда даёт тот же id,
     * поэтому повторная выгрузка порождает «тот же» event — это упрощает дедупликацию и трассировку.
     */
    static UUID eventId(String eventType, ObjectNode payload) {
        String version = payload.hasNonNull("data_version") ? payload.get("data_version").asText()
                : String.valueOf(payload.get("updated_at"));
        String seed = eventType + "|" + payload.get("id").asText() + "|" + version + "|" + payload.get("deleted").asBoolean();
        return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() {
        producer.close();
    }
}
