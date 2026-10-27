package ru.integration.onec.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ru.integration.onec.Config;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Описание выгружаемых справочников: имя набора OData, топик, тип события
 * и преобразование записи 1С в единый JSON-формат payload.
 */
public enum Catalog {

    OWNERSHIP_FORMS("ownership_forms", "Catalog_ФормыСобственности", "ownership_form.upsert",
            List.of("Ref_Key", "Code", "Description", "DeletionMark", "ДатаИзменения", "DataVersion")),

    COUNTERPARTIES("counterparties", "Catalog_Контрагенты", "counterparty.upsert",
            List.of("Ref_Key", "Code", "Description", "ИНН", "КПП", "ФормаСобственности_Key",
                    "DeletionMark", "ДатаИзменения", "DataVersion"));

    private static final String EMPTY_REF = "00000000-0000-0000-0000-000000000000";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public final String key;
    public final String entitySet;
    public final String eventType;
    public final List<String> select;

    Catalog(String key, String entitySet, String eventType, List<String> select) {
        this.key = key;
        this.entitySet = entitySet;
        this.eventType = eventType;
        this.select = select;
    }

    public String topic(Config config) {
        return this == OWNERSHIP_FORMS ? config.topicOwnershipForms() : config.topicCounterparties();
    }

    public static Catalog byKey(String key) {
        for (Catalog c : values()) {
            if (c.key.equals(key)) {
                return c;
            }
        }
        throw new IllegalArgumentException("Unknown entity: " + key + " (expected ownership_forms or counterparties)");
    }

    /** Преобразует запись OData 1С в payload события. */
    public ObjectNode toPayload(JsonNode row) {
        ObjectNode p = MAPPER.createObjectNode();
        String id = text(row, "Ref_Key");
        if (id == null || id.equals(EMPTY_REF)) {
            throw new IllegalArgumentException("Record without Ref_Key in " + entitySet);
        }
        p.put("id", id);
        p.put("code", text(row, "Code"));
        p.put("name", text(row, "Description"));
        if (this == COUNTERPARTIES) {
            p.put("inn", text(row, "ИНН"));
            p.put("kpp", text(row, "КПП"));
            String form = text(row, "ФормаСобственности_Key");
            p.put("ownership_form_id", form == null || form.equals(EMPTY_REF) ? null : form);
        }
        p.put("deleted", row.path("DeletionMark").asBoolean(false));
        LocalDateTime updated = updatedAt(row);
        p.put("updated_at", updated == null ? null : updated.atOffset(ZoneOffset.UTC).toInstant().toString());
        p.put("data_version", text(row, "DataVersion"));
        return p;
    }

    /** Значение ДатаИзменения (в 1С хранится в UTC); пустая дата 1С — 0001-01-01. */
    public static LocalDateTime updatedAt(JsonNode row) {
        String value = text(row, "ДатаИзменения");
        if (value == null || value.startsWith("0001-01-01")) {
            return null;
        }
        return LocalDateTime.parse(value);
    }

    private static String text(JsonNode row, String field) {
        JsonNode node = row.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        String value = node.asText().trim();
        return value.isEmpty() ? null : value;
    }
}
