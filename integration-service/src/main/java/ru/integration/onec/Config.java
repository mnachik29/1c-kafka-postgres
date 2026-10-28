package ru.integration.onec;

import java.nio.file.Path;
import java.time.Duration;

/**
 * Все настройки берутся из переменных окружения; секреты в коде не хранятся.
 */
public record Config(
        String odataUrl,
        String odataUser,
        String odataPassword,
        Duration httpTimeout,
        int pageSize,
        int retryMaxAttempts,
        Duration retryInitialBackoff,
        String kafkaBootstrap,
        String topicOwnershipForms,
        String topicCounterparties,
        Path stateDir,
        Duration incrementalOverlap,
        int healthPort,
        Duration scheduleInterval,
        String syncApiToken
) {

    public static Config fromEnv() {
        return new Config(
                trimSlash(required("ONEC_ODATA_URL")),
                env("ONEC_USER", ""),
                env("ONEC_PASSWORD", ""),
                Duration.ofSeconds(intEnv("ONEC_HTTP_TIMEOUT_SECONDS", 30)),
                intEnv("ONEC_PAGE_SIZE", 500),
                intEnv("RETRY_MAX_ATTEMPTS", 5),
                Duration.ofMillis(intEnv("RETRY_INITIAL_BACKOFF_MS", 1000)),
                required("KAFKA_BOOTSTRAP_SERVERS"),
                env("TOPIC_OWNERSHIP_FORMS", "1c.ownership_forms.v1"),
                env("TOPIC_COUNTERPARTIES", "1c.counterparties.v1"),
                Path.of(env("STATE_DIR", "/data")),
                Duration.ofSeconds(intEnv("SYNC_OVERLAP_SECONDS", 5)),
                intEnv("HEALTH_PORT", 8081),
                Duration.ofSeconds(intEnv("SYNC_INTERVAL_SECONDS", 0)),
                env("SYNC_API_TOKEN", "")
        );
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

    private static String trimSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }
}
