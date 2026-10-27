package ru.integration.sink.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.UUID;

/**
 * Разбирает JSON-конверт события и проверяет обязательные поля.
 * Невалидные сообщения не повторяются, а уходят в DLQ.
 */
public class EventParser {

    public static class InvalidEventException extends Exception {
        public InvalidEventException(String message) {
            super(message);
        }
    }

    private final ObjectMapper mapper;

    public EventParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public CatalogEvent parse(String value) throws InvalidEventException {
        if (value == null) {
            throw new InvalidEventException("Empty message (tombstone) is not supported");
        }
        JsonNode event;
        try {
            event = mapper.readTree(value);
        } catch (Exception e) {
            throw new InvalidEventException("Malformed JSON: " + e.getMessage());
        }
        UUID eventId = uuid(event, "event_id");
        String type = requiredText(event, "event_type");
        JsonNode p = event.get("payload");
        if (p == null || !p.isObject()) {
            throw new InvalidEventException("Missing payload");
        }
        return switch (type) {
            case "ownership_form.upsert" -> new CatalogEvent.OwnershipForm(
                    eventId, requiredText(p, "id"), text(p, "code"), requiredText(p, "name"),
                    bool(p), timestamp(p), text(p, "data_version"));
            case "counterparty.upsert" -> new CatalogEvent.Counterparty(
                    eventId, uuid(p, "id"), text(p, "code"), requiredText(p, "name"),
                    text(p, "inn"), text(p, "kpp"), text(p, "ownership_form_id"),
                    bool(p), timestamp(p), text(p, "data_version"));
            default -> throw new InvalidEventException("Unsupported event_type: " + type);
        };
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() || v.asText().isBlank() ? null : v.asText();
    }

    private static String requiredText(JsonNode node, String field) throws InvalidEventException {
        String v = text(node, field);
        if (v == null) {
            throw new InvalidEventException("Missing required field: " + field);
        }
        return v;
    }

    private static UUID uuid(JsonNode node, String field) throws InvalidEventException {
        String v = requiredText(node, field);
        try {
            return UUID.fromString(v);
        } catch (IllegalArgumentException e) {
            throw new InvalidEventException("Field " + field + " is not a UUID: " + v);
        }
    }

    private static boolean bool(JsonNode node) throws InvalidEventException {
        JsonNode v = node.get("deleted");
        if (v == null || v.isNull()) {
            return false;
        }
        if (!v.isBoolean()) {
            throw new InvalidEventException("Field deleted must be boolean");
        }
        return v.asBoolean();
    }

    private static OffsetDateTime timestamp(JsonNode node) throws InvalidEventException {
        String v = text(node, "updated_at");
        if (v == null) {
            return null;
        }
        try {
            return OffsetDateTime.parse(v);
        } catch (DateTimeParseException e) {
            throw new InvalidEventException("Field updated_at is not ISO-8601: " + v);
        }
    }
}
