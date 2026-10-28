package ru.integration.onec.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Хранит по каждому справочнику в JSON-файле на томе контейнера:
 * <ul>
 *   <li>метку инкрементальной синхронизации — максимальную ДатаИзменения, которую уже успешно опубликовали;</li>
 *   <li>версии записей на границе окна (Ref_Key → DataVersion), чтобы перекрытие окна не отправляло их повторно.</li>
 * </ul>
 * <pre>
 * {"counterparties": {"watermark": "2026-10-28T07:00:33", "boundary": {"e27cb1f5-…": "AAAABgAAAAA="}}}
 * </pre>
 */
public class StateStore {

    /** Метка и уже отправленные версии у её границы. */
    public record Checkpoint(LocalDateTime watermark, Map<String, String> boundary) {
        public static final Checkpoint EMPTY = new Checkpoint(null, Map.of());
    }

    private final Path file;
    private final ObjectMapper mapper;

    public StateStore(Path dir, ObjectMapper mapper) {
        this.file = dir.resolve("sync-state.json");
        this.mapper = mapper;
    }

    public synchronized Checkpoint checkpoint(Catalog catalog) throws IOException {
        JsonNode node = read().path(catalog.key);
        if (node.isTextual()) {
            // прежний формат: только метка
            return new Checkpoint(LocalDateTime.parse(node.asText()), Map.of());
        }
        if (!node.isObject() || !node.hasNonNull("watermark")) {
            return Checkpoint.EMPTY;
        }
        Map<String, String> boundary = new HashMap<>();
        node.path("boundary").fields().forEachRemaining(e -> boundary.put(e.getKey(), e.getValue().asText()));
        return new Checkpoint(LocalDateTime.parse(node.get("watermark").asText()), boundary);
    }

    public synchronized void save(Catalog catalog, Checkpoint checkpoint) throws IOException {
        ObjectNode state = read();
        ObjectNode node = state.putObject(catalog.key);
        node.put("watermark", checkpoint.watermark().toString());
        ObjectNode boundary = node.putObject("boundary");
        checkpoint.boundary().forEach(boundary::put);

        Files.createDirectories(file.getParent());
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), state);
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    public synchronized void reset() throws IOException {
        Files.deleteIfExists(file);
    }

    private ObjectNode read() throws IOException {
        if (!Files.exists(file)) {
            return mapper.createObjectNode();
        }
        return (ObjectNode) mapper.readTree(file.toFile());
    }
}
