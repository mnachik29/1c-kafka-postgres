package ru.integration.onec.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;

/**
 * Хранит метку инкрементальной синхронизации (максимальная ДатаИзменения, которую уже
 * успешно опубликовали) по каждому справочнику в JSON-файле на томе контейнера.
 */
public class StateStore {

    private final Path file;
    private final ObjectMapper mapper;

    public StateStore(Path dir, ObjectMapper mapper) {
        this.file = dir.resolve("sync-state.json");
        this.mapper = mapper;
    }

    public synchronized LocalDateTime watermark(Catalog catalog) throws IOException {
        ObjectNode state = read();
        String value = state.path(catalog.key).asText(null);
        return value == null ? null : LocalDateTime.parse(value);
    }

    public synchronized void saveWatermark(Catalog catalog, LocalDateTime value) throws IOException {
        ObjectNode state = read();
        state.put(catalog.key, value.toString());
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
