package ru.integration.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Имитация стандартного OData-интерфейса 1С для тех, у кого не установлен модуль веб-сервера.
 * Поддерживает то подмножество протокола, которое использует integration-service и демо-сценарий:
 * <pre>
 *   GET   {base}/$metadata
 *   GET   {base}/Catalog_X?$format=json&$filter=ДатаИзменения ge datetime'...'&$top=&$skip=
 *   GET   {base}/Catalog_X(guid'...')
 *   PATCH {base}/Catalog_X(guid'...')   — изменить реквизиты / DeletionMark
 *   POST  {base}/Catalog_X              — создать элемент
 * </pre>
 * Поведение как в нашей конфигурации 1С: при любой записи ДатаИзменения = текущее UTC-время.
 */
public final class ODataMock {

    private static final String BASE = "/demo1c/odata/standard.odata";
    private static final Pattern BY_KEY = Pattern.compile("^(Catalog_[^(/]+)\\(guid'([0-9a-fA-F-]{36})'\\)$");
    private static final Pattern FILTER_SINCE = Pattern.compile("ДатаИзменения\\s+(ge|gt)\\s+datetime'([^']+)'");

    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, Map<String, ObjectNode>> sets = new HashMap<>();
    private final Map<String, Integer> codeCounters = new HashMap<>();

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
        ODataMock mock = new ODataMock();
        mock.seed();
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", mock::handle);
        server.start();
        System.out.println("1C OData mock listening on :" + port + BASE);
    }

    private void seed() {
        sets.put("Catalog_ФормыСобственности", new LinkedHashMap<>());
        sets.put("Catalog_Контрагенты", new LinkedHashMap<>());
        String ooo = form("8f0a7a10-0000-4000-8000-000000000001", "ООО");
        String ip = form("8f0a7a10-0000-4000-8000-000000000002", "ИП");
        String ao = form("8f0a7a10-0000-4000-8000-000000000003", "АО");
        String pao = form("8f0a7a10-0000-4000-8000-000000000004", "ПАО");
        counterparty("b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001", "ООО «Ромашка»", "7701234567", "770101001", ooo);
        counterparty("b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0002", "ИП Иванов И.И.", "500100732259", "", ip);
        counterparty("b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0003", "АО «Север»", "7802345678", "780201001", ao);
        counterparty("b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0004", "ПАО «Восток»", "2501234567", "250101001", pao);
        counterparty("b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0005", "ООО «Лютик»", "7703456789", "770301001", ooo);
        counterparty("b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0006", "ИП Петрова А.С.", "771234567890", "", ip);
    }

    private String form(String id, String name) {
        ObjectNode o = mapper.createObjectNode();
        o.put("Ref_Key", id);
        o.put("Description", name);
        touch("Catalog_ФормыСобственности", o, true);
        sets.get("Catalog_ФормыСобственности").put(id, o);
        return id;
    }

    private void counterparty(String id, String name, String inn, String kpp, String form) {
        ObjectNode o = mapper.createObjectNode();
        o.put("Ref_Key", id);
        o.put("Description", name);
        o.put("ИНН", inn);
        o.put("КПП", kpp);
        o.put("ФормаСобственности_Key", form);
        touch("Catalog_Контрагенты", o, true);
        sets.get("Catalog_Контрагенты").put(id, o);
    }

    /** Аналог ПередЗаписью в модуле объекта: технические поля при каждой записи. */
    private void touch(String setName, ObjectNode o, boolean isNew) {
        if (isNew) {
            o.put("Code", String.format("%09d", codeCounters.merge(setName, 1, Integer::sum)));
            if (!o.has("DeletionMark")) {
                o.put("DeletionMark", false);
            }
        }
        o.put("ДатаИзменения", LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS).toString());
        o.put("DataVersion", Base64.getEncoder().encodeToString(UUID.randomUUID().toString().substring(0, 8)
                .getBytes(StandardCharsets.UTF_8)));
    }

    private synchronized void handle(HttpExchange ex) throws IOException {
        try {
            String path = ex.getRequestURI().getPath();
            if (path.equals("/health")) {
                send(ex, 200, "{\"status\":\"UP\"}");
                return;
            }
            if (!path.startsWith(BASE + "/")) {
                error(ex, 404, "Not found: " + path);
                return;
            }
            String resource = path.substring(BASE.length() + 1);
            if (resource.equals("$metadata")) {
                send(ex, 200, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><edmx:Edmx Version=\"1.0\" "
                        + "xmlns:edmx=\"http://schemas.microsoft.com/ado/2007/06/edmx\"/>", "application/xml");
                return;
            }
            Matcher byKey = BY_KEY.matcher(resource);
            String setName = byKey.matches() ? byKey.group(1) : resource;
            Map<String, ObjectNode> set = sets.get(setName);
            if (set == null) {
                error(ex, 404, "Entity set is not published: " + setName);
                return;
            }
            String method = ex.getRequestMethod();
            if (byKey.matches()) {
                ObjectNode item = set.get(byKey.group(2).toLowerCase());
                if (item == null) {
                    error(ex, 404, "Object not found");
                    return;
                }
                if (method.equals("PATCH") || method.equals("MERGE")) {
                    merge(item, readBody(ex));
                    touch(setName, item, false);
                }
                send(ex, 200, withMetadata(setName, item, true));
            } else if (method.equals("POST")) {
                ObjectNode item = mapper.createObjectNode();
                item.put("Ref_Key", UUID.randomUUID().toString());
                merge(item, readBody(ex));
                touch(setName, item, true);
                set.put(item.get("Ref_Key").asText(), item);
                send(ex, 201, withMetadata(setName, item, true));
            } else {
                list(ex, setName, set);
            }
        } catch (Exception e) {
            error(ex, 500, e.toString());
        }
    }

    private void list(HttpExchange ex, String setName, Map<String, ObjectNode> set) throws IOException {
        Map<String, String> q = query(ex.getRequestURI().getRawQuery());
        List<ObjectNode> items = new ArrayList<>(set.values());
        String filter = q.get("$filter");
        if (filter != null) {
            Matcher m = FILTER_SINCE.matcher(filter);
            if (!m.find()) {
                error(ex, 400, "Mock supports only filter on ДатаИзменения: " + filter);
                return;
            }
            LocalDateTime since = LocalDateTime.parse(m.group(2));
            boolean inclusive = m.group(1).equals("ge");
            items.removeIf(i -> {
                LocalDateTime d = LocalDateTime.parse(i.get("ДатаИзменения").asText());
                return inclusive ? d.isBefore(since) : !d.isAfter(since);
            });
        }
        items.sort(Comparator.comparing((ObjectNode i) -> i.get("ДатаИзменения").asText())
                .thenComparing(i -> i.get("Ref_Key").asText()));
        int skip = Integer.parseInt(q.getOrDefault("$skip", "0"));
        int top = Integer.parseInt(q.getOrDefault("$top", String.valueOf(Integer.MAX_VALUE)));
        ArrayNode value = mapper.createArrayNode();
        items.stream().skip(skip).limit(top).forEach(value::add);
        ObjectNode body = mapper.createObjectNode();
        body.put("odata.metadata", "http://localhost" + BASE + "/$metadata#" + setName);
        body.set("value", value);
        send(ex, 200, mapper.writeValueAsString(body));
    }

    private void merge(ObjectNode item, JsonNode patch) {
        patch.fields().forEachRemaining(e -> {
            if (!e.getKey().equals("Ref_Key") && !e.getKey().startsWith("odata.")) {
                item.set(e.getKey(), e.getValue());
            }
        });
    }

    private String withMetadata(String setName, ObjectNode item, boolean entry) throws IOException {
        ObjectNode copy = item.deepCopy();
        copy.put("odata.metadata", "http://localhost" + BASE + "/$metadata#" + setName + (entry ? "/@Element" : ""));
        return mapper.writeValueAsString(copy);
    }

    private JsonNode readBody(HttpExchange ex) throws IOException {
        byte[] bytes = ex.getRequestBody().readAllBytes();
        return bytes.length == 0 ? mapper.createObjectNode() : mapper.readTree(bytes);
    }

    private static Map<String, String> query(String raw) {
        Map<String, String> result = new HashMap<>();
        if (raw == null) {
            return result;
        }
        for (String pair : raw.split("&")) {
            int i = pair.indexOf('=');
            String k = URLDecoder.decode(i < 0 ? pair : pair.substring(0, i), StandardCharsets.UTF_8);
            String v = i < 0 ? "" : URLDecoder.decode(pair.substring(i + 1), StandardCharsets.UTF_8);
            result.put(k, v);
        }
        return result;
    }

    private void error(HttpExchange ex, int status, String message) throws IOException {
        ObjectNode body = mapper.createObjectNode();
        body.putObject("odata.error").putObject("message").put("value", message);
        send(ex, status, mapper.writeValueAsString(body));
    }

    private static void send(HttpExchange ex, int status, String body) throws IOException {
        send(ex, status, body, "application/json;charset=utf-8");
    }

    private static void send(HttpExchange ex, int status, String body, String contentType) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", contentType);
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }
}
