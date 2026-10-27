package ru.integration.onec.odata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.integration.onec.Config;
import ru.integration.onec.Retry;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Consumer;

import static net.logstash.logback.argument.StructuredArguments.kv;

/**
 * Клиент стандартного OData-интерфейса 1С (OData v3, JSON).
 * Пример запроса: GET {base}/Catalog_Контрагенты?$format=json&$filter=ДатаИзменения ge datetime'2026-07-10T12:00:00'
 */
public class ODataClient {

    private static final Logger log = LoggerFactory.getLogger(ODataClient.class);
    private static final DateTimeFormatter ODATA_DATETIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    private final Config config;
    private final HttpClient http;
    private final ObjectMapper mapper;
    private final String authHeader;

    public ODataClient(Config config, ObjectMapper mapper) {
        this.config = config;
        this.mapper = mapper;
        this.http = HttpClient.newBuilder()
                .connectTimeout(config.httpTimeout())
                .build();
        this.authHeader = config.odataUser().isEmpty() ? null
                : "Basic " + Base64.getEncoder().encodeToString(
                (config.odataUser() + ":" + config.odataPassword()).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Постранично читает набор сущностей и передаёт каждую страницу обработчику.
     *
     * @param entitySet     например {@code Catalog_Контрагенты}
     * @param select        список полей для $select
     * @param changedSince  если не null — только записи с ДатаИзменения &gt;= значения
     * @return общее число прочитанных записей
     */
    public int readAll(String entitySet, List<String> select, LocalDateTime changedSince,
                       Consumer<List<JsonNode>> pageHandler) throws Exception {
        int skip = 0;
        int total = 0;
        while (true) {
            StringBuilder query = new StringBuilder("$format=json")
                    .append("&$select=").append(encode(String.join(",", select)))
                    .append("&$orderby=").append(encode("ДатаИзменения,Ref_Key"))
                    .append("&$top=").append(config.pageSize())
                    .append("&$skip=").append(skip);
            if (changedSince != null) {
                query.append("&$filter=").append(encode(
                        "ДатаИзменения ge datetime'" + ODATA_DATETIME.format(changedSince) + "'"));
            }
            URI uri = URI.create(config.odataUrl() + "/" + encode(entitySet) + "?" + query);
            JsonNode body = Retry.call("odata.get " + entitySet, config.retryMaxAttempts(),
                    config.retryInitialBackoff(), () -> get(uri));

            JsonNode value = body.path("value");
            if (!value.isArray()) {
                throw new IllegalStateException("Unexpected OData response for " + entitySet + ": no 'value' array");
            }
            List<JsonNode> page = new ArrayList<>(value.size());
            value.forEach(page::add);
            log.info("OData page received", kv("entity_set", entitySet), kv("skip", skip), kv("count", page.size()));
            if (!page.isEmpty()) {
                pageHandler.accept(page);
            }
            total += page.size();
            if (page.size() < config.pageSize()) {
                return total;
            }
            skip += page.size();
        }
    }

    /** Проверка доступности 1С для healthcheck. */
    public boolean ping() {
        try {
            HttpRequest request = request(URI.create(config.odataUrl() + "/$metadata")).build();
            return http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private JsonNode get(URI uri) throws IOException, InterruptedException {
        HttpRequest request = request(uri).header("Accept", "application/json").GET().build();
        HttpResponse<String> response;
        try {
            response = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IOException("1C OData is unreachable at " + config.odataUrl() + ": " + e, e);
        }
        int status = response.statusCode();
        if (status == 200) {
            return mapper.readTree(response.body());
        }
        String snippet = response.body() == null ? "" : response.body().substring(0, Math.min(500, response.body().length()));
        String message = "1C OData returned HTTP " + status + " for " + uri + ": " + snippet;
        if (status == 401 || status == 403 || status == 400 || status == 404) {
            // повторять бессмысленно: неверные учётные данные, не опубликован объект или ошибка запроса
            throw new Retry.NonRetryableException(message);
        }
        throw new IOException(message);
    }

    private HttpRequest.Builder request(URI uri) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri).timeout(config.httpTimeout());
        if (authHeader != null) {
            builder.header("Authorization", authHeader);
        }
        return builder;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
