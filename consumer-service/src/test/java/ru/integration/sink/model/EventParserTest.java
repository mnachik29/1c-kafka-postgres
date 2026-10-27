package ru.integration.sink.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import ru.integration.sink.Config;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventParserTest {

    private final EventParser parser = new EventParser(new ObjectMapper());

    @Test
    void parsesCounterpartyEvent() throws Exception {
        CatalogEvent event = parser.parse("""
                {"event_id":"9d3c8f6c-21a5-4f87-8b8e-8e2f3c7c1001","event_type":"counterparty.upsert","source":"1c",
                 "occurred_at":"2026-07-10T12:30:00Z",
                 "payload":{"id":"b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001","code":"000001","name":"ООО Ромашка",
                 "inn":"7701234567","kpp":"770101001","ownership_form_id":"ooo","deleted":true,
                 "updated_at":"2026-07-10T12:00:00Z"}}""");

        CatalogEvent.Counterparty cp = assertInstanceOf(CatalogEvent.Counterparty.class, event);
        assertEquals("ООО Ромашка", cp.name());
        assertEquals("ooo", cp.ownershipFormId());
        assertTrue(cp.deleted());
        assertEquals("2026-07-10T12:00Z", cp.updatedAt().toString());
    }

    @Test
    void rejectsInvalidEvents() {
        assertThrows(EventParser.InvalidEventException.class, () -> parser.parse("not json"));
        assertThrows(EventParser.InvalidEventException.class, () -> parser.parse(null));
        assertThrows(EventParser.InvalidEventException.class, () -> parser.parse("""
                {"event_id":"9d3c8f6c-21a5-4f87-8b8e-8e2f3c7c1001","event_type":"counterparty.upsert",
                 "payload":{"id":"not-a-uuid","name":"x"}}"""));
        assertThrows(EventParser.InvalidEventException.class, () -> parser.parse("""
                {"event_id":"9d3c8f6c-21a5-4f87-8b8e-8e2f3c7c1001","event_type":"ownership_form.upsert",
                 "payload":{"id":"ooo"}}"""));
    }

    @Test
    void dlqTopicName() {
        Config config = new Config("k", "g", List.of(), 1, "j", "u", "p", 1, Duration.ZERO, ".dlq", 0, "x");
        assertEquals("1c.counterparties.dlq", config.dlqTopic("1c.counterparties.v1"));
        assertEquals("1c.ownership_forms.dlq", config.dlqTopic("1c.ownership_forms.v1"));
    }
}
