package ru.integration.onec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Callable;

import static net.logstash.logback.argument.StructuredArguments.kv;

/**
 * Повтор с ограниченным числом попыток и экспоненциальной задержкой.
 */
public final class Retry {

    private static final Logger log = LoggerFactory.getLogger(Retry.class);

    /** Ошибка, повторять которую бессмысленно (например, HTTP 400/404). */
    public static class NonRetryableException extends RuntimeException {
        public NonRetryableException(String message) {
            super(message);
        }
    }

    private Retry() {
    }

    public static <T> T call(String operation, int maxAttempts, Duration initialBackoff, Callable<T> action) throws Exception {
        long backoffMs = initialBackoff.toMillis();
        for (int attempt = 1; ; attempt++) {
            try {
                return action.call();
            } catch (NonRetryableException e) {
                throw e;
            } catch (Exception e) {
                if (attempt >= maxAttempts) {
                    log.error("Operation failed, no attempts left",
                            kv("operation", operation), kv("attempt", attempt), kv("error", e.toString()));
                    throw e;
                }
                log.warn("Operation failed, will retry",
                        kv("operation", operation), kv("attempt", attempt), kv("max_attempts", maxAttempts),
                        kv("backoff_ms", backoffMs), kv("error", e.toString()));
                Thread.sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 30_000);
            }
        }
    }
}
