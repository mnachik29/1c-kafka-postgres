package ru.integration.onec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import ru.integration.onec.odata.ODataClient;
import ru.integration.onec.sync.SyncService;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * GET /health  — состояние сервиса и доступность 1С;
 * GET /metrics — простые счётчики в формате Prometheus.
 */
public class HealthServer {

    private final ObjectMapper mapper;
    private final ODataClient odata;
    private final AtomicReference<List<SyncService.Result>> lastResults = new AtomicReference<>(List.of());
    private final AtomicReference<String> lastError = new AtomicReference<>();
    private final AtomicReference<Instant> lastRunAt = new AtomicReference<>();
    private final AtomicLong runsOk = new AtomicLong();
    private final AtomicLong runsFailed = new AtomicLong();
    private final AtomicLong eventsPublished = new AtomicLong();
    private HttpServer server;

    public HealthServer(ObjectMapper mapper, ODataClient odata) {
        this.mapper = mapper;
        this.odata = odata;
    }

    public void start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", this::health);
        server.createContext("/metrics", this::metrics);
        server.start();
    }

    public void recordSuccess(List<SyncService.Result> results) {
        lastResults.set(results);
        lastError.set(null);
        lastRunAt.set(Instant.now());
        runsOk.incrementAndGet();
        results.forEach(r -> eventsPublished.addAndGet(r.published()));
    }

    public void recordFailure(Exception e) {
        lastError.set(e.toString());
        lastRunAt.set(Instant.now());
        runsFailed.incrementAndGet();
    }

    private void health(HttpExchange exchange) throws IOException {
        ObjectNode body = mapper.createObjectNode();
        body.put("status", "UP");
        body.put("onec", odata.ping() ? "UP" : "DOWN");
        body.put("last_run_at", lastRunAt.get() == null ? null : lastRunAt.get().toString());
        body.put("last_error", lastError.get());
        body.set("last_results", mapper.valueToTree(lastResults.get()));
        send(exchange, 200, "application/json", mapper.writeValueAsString(body));
    }

    private void metrics(HttpExchange exchange) throws IOException {
        String text = "sync_runs_total{result=\"ok\"} " + runsOk.get() + "\n"
                + "sync_runs_total{result=\"failed\"} " + runsFailed.get() + "\n"
                + "sync_events_published_total " + eventsPublished.get() + "\n";
        send(exchange, 200, "text/plain; version=0.0.4", text);
    }

    private static void send(HttpExchange exchange, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
