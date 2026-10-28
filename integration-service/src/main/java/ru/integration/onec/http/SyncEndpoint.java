package ru.integration.onec.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.integration.onec.sync.Catalog;
import ru.integration.onec.sync.SyncService;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.logstash.logback.argument.StructuredArguments.kv;

/**
 * POST /sync?mode=full|incremental[&entity=all|ownership_forms|counterparties][&since=2026-07-10T12:00:00][&reset=true]
 * — синхронно запускает синхронизацию и возвращает её итог (те же параметры, что у CLI `sync`).
 * Если задан SYNC_API_TOKEN, нужен заголовок {@code Authorization: Bearer <token>}.
 */
public class SyncEndpoint implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(SyncEndpoint.class);

    private final ObjectMapper mapper;
    private final SyncService sync;
    private final String token;

    public SyncEndpoint(ObjectMapper mapper, SyncService sync, String token) {
        this.mapper = mapper;
        this.sync = sync;
        this.token = token;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("POST")) {
            exchange.getResponseHeaders().set("Allow", "POST");
            error(exchange, 405, "Use POST");
            return;
        }
        if (!token.isEmpty() && !authorized(exchange)) {
            error(exchange, 401, "Missing or invalid Authorization: Bearer token");
            return;
        }

        SyncService.Mode mode;
        List<Catalog> catalogs;
        LocalDateTime since;
        boolean reset;
        try {
            Map<String, String> params = query(exchange);
            mode = switch (params.getOrDefault("mode", "")) {
                case "full" -> SyncService.Mode.FULL;
                case "incremental" -> SyncService.Mode.INCREMENTAL;
                default -> throw new IllegalArgumentException("mode must be full or incremental");
            };
            catalogs = SyncService.catalogs(params.get("entity"));
            since = params.containsKey("since") ? LocalDateTime.parse(params.get("since")) : null;
            reset = Boolean.parseBoolean(params.get("reset"));
        } catch (RuntimeException e) {
            error(exchange, 400, e.getMessage());
            return;
        }

        log.info("Sync requested via HTTP", kv("mode", mode), kv("entities", catalogs.stream().map(c -> c.key).toList()),
                kv("since", String.valueOf(since)), kv("reset", reset));
        try {
            List<SyncService.Result> results = sync.run(mode, catalogs, since, reset);
            ObjectNode body = mapper.createObjectNode();
            body.put("status", "OK");
            body.set("results", mapper.valueToTree(results));
            HttpApi.send(exchange, 200, "application/json", mapper.writeValueAsString(body));
        } catch (SyncService.AlreadyRunningException e) {
            error(exchange, 409, e.getMessage());
        } catch (Exception e) {
            // уже залогировано в SyncService; метка не сдвинута, запрос можно повторить
            error(exchange, 500, e.toString());
        }
    }

    private boolean authorized(HttpExchange exchange) {
        String header = exchange.getRequestHeaders().getFirst("Authorization");
        String expected = "Bearer " + token;
        return header != null && MessageDigest.isEqual(
                header.getBytes(StandardCharsets.UTF_8), expected.getBytes(StandardCharsets.UTF_8));
    }

    private static Map<String, String> query(HttpExchange exchange) {
        Map<String, String> params = new HashMap<>();
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null || raw.isEmpty()) {
            return params;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            String key = URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), StandardCharsets.UTF_8);
            String value = eq < 0 ? "true" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            params.put(key, value);
        }
        return params;
    }

    private void error(HttpExchange exchange, int status, String message) throws IOException {
        ObjectNode body = mapper.createObjectNode();
        body.put("status", "ERROR");
        body.put("error", message);
        HttpApi.send(exchange, status, "application/json", mapper.writeValueAsString(body));
    }
}
