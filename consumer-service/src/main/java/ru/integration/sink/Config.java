package ru.integration.sink;

import java.time.Duration;
import java.util.List;

/**
 * Все настройки берутся из переменных окружения; секреты в коде не хранятся.
 */
public record Config(
        String kafkaBootstrap,
        String groupId,
        List<String> topics,
        int maxPollRecords,
        String jdbcUrl,
        String dbUser,
        String dbPassword,
        int retryMaxAttempts,
        Duration retryInitialBackoff,
        String dlqSuffix,
        int healthPort,
        String migrationsLocation
) {

    public static Config fromEnv() {
        return new Config(
                required("KAFKA_BOOTSTRAP_SERVERS"),
                env("KAFKA_GROUP_ID", "pg-sink"),
                List.of(env("TOPIC_OWNERSHIP_FORMS", "1c.ownership_forms.v1"),
                        env("TOPIC_COUNTERPARTIES", "1c.counterparties.v1")),
                intEnv("KAFKA_MAX_POLL_RECORDS", 500),
                required("POSTGRES_JDBC_URL"),
                required("POSTGRES_USER"),
                required("POSTGRES_PASSWORD"),
                intEnv("RETRY_MAX_ATTEMPTS", 5),
                Duration.ofMillis(intEnv("RETRY_INITIAL_BACKOFF_MS", 1000)),
                env("DLQ_SUFFIX", ".dlq"),
                intEnv("HEALTH_PORT", 8082),
                env("MIGRATIONS_LOCATION", "filesystem:/app/migrations")
        );
    }

    /** 1c.counterparties.v1 -> 1c.counterparties.dlq */
    public String dlqTopic(String topic) {
        String base = topic.replaceFirst("\\.v\\d+$", "");
        return base + dlqSuffix;
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Environment variable " + name + " is required");
        }
        return value.trim();
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    private static int intEnv(String name, int defaultValue) {
        String value = env(name, null);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Environment variable " + name + " must be an integer, got: " + value);
        }
    }
}
