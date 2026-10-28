package ru.integration.onec.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StateStoreTest {

    @TempDir
    Path dir;

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void savesWatermarkAndBoundaryPerCatalog() throws Exception {
        StateStore store = new StateStore(dir, mapper);
        LocalDateTime watermark = LocalDateTime.parse("2026-10-28T07:00:33");
        store.save(Catalog.COUNTERPARTIES, new StateStore.Checkpoint(watermark, Map.of("ref-1", "v1")));

        StateStore.Checkpoint read = new StateStore(dir, mapper).checkpoint(Catalog.COUNTERPARTIES);

        assertEquals(watermark, read.watermark());
        assertEquals(Map.of("ref-1", "v1"), read.boundary());
        assertNull(store.checkpoint(Catalog.OWNERSHIP_FORMS).watermark(), "other catalog is untouched");
    }

    @Test
    void readsLegacyFormatWithWatermarkOnly() throws Exception {
        Files.writeString(dir.resolve("sync-state.json"), "{\"counterparties\":\"2026-10-28T07:00:33\"}");

        StateStore.Checkpoint read = new StateStore(dir, mapper).checkpoint(Catalog.COUNTERPARTIES);

        assertEquals(LocalDateTime.parse("2026-10-28T07:00:33"), read.watermark());
        assertTrue(read.boundary().isEmpty());
    }

    @Test
    void resetForgetsEverything() throws Exception {
        StateStore store = new StateStore(dir, mapper);
        store.save(Catalog.OWNERSHIP_FORMS,
                new StateStore.Checkpoint(LocalDateTime.parse("2026-10-27T07:49:07"), Map.of()));

        store.reset();

        assertEquals(StateStore.Checkpoint.EMPTY, store.checkpoint(Catalog.OWNERSHIP_FORMS));
    }
}
