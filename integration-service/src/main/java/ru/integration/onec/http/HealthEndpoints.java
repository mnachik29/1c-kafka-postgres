package ru.integration.onec.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import ru.integration.onec.odata.ODataClient;
import ru.integration.onec.sync.SyncStats;

import java.io.IOException;

/**
 * GET /health  — состояние сервиса, доступность 1С и итог последней синхронизации;
 * GET /metrics — простые счётчики в формате Prometheus.
 */
public class HealthEndpoints {

    private final ObjectMapper mapper;
    private final ODataClient odata;
    private final SyncStats stats;

    public HealthEndpoints(ObjectMapper mapper, ODataClient odata, SyncStats stats) {
        this.mapper = mapper;
        this.odata = odata;
        this.stats = stats;
    }

    public void health(HttpExchange exchange) throws IOException {
        SyncStats.LastRun last = stats.lastRun();
        ObjectNode body = mapper.createObjectNode();
        body.put("status", "UP");
        body.put("onec", odata.ping() ? "UP" : "DOWN");
        body.put("last_run_at", last.at() == null ? null : last.at().toString());
        body.put("last_error", last.error());
        body.set("last_results", mapper.valueToTree(last.results()));
        HttpApi.send(exchange, 200, "application/json", mapper.writeValueAsString(body));
    }

    public void metrics(HttpExchange exchange) throws IOException {
        String text = "sync_runs_total{result=\"ok\"} " + stats.runsOk() + "\n"
                + "sync_runs_total{result=\"failed\"} " + stats.runsFailed() + "\n"
                + "sync_events_published_total " + stats.eventsPublished() + "\n";
        HttpApi.send(exchange, 200, "text/plain; version=0.0.4", text);
    }
}
