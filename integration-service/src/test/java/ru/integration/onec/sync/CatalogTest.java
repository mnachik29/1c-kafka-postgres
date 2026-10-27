package ru.integration.onec.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CatalogTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void mapsCounterpartyFromOData() throws Exception {
        JsonNode row = mapper.readTree("""
                {"Ref_Key":"b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001","DataVersion":"AAAAAQAAAAA=",
                 "DeletionMark":false,"Code":"000000001","Description":"ООО «Ромашка»",
                 "ИНН":"7701234567","КПП":"","ФормаСобственности_Key":"00000000-0000-0000-0000-000000000000",
                 "ДатаИзменения":"2026-07-10T12:00:00"}""");

        ObjectNode p = Catalog.COUNTERPARTIES.toPayload(row);

        assertEquals("b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001", p.get("id").asText());
        assertEquals("ООО «Ромашка»", p.get("name").asText());
        assertTrue(p.get("kpp").isNull(), "empty string from 1C becomes null");
        assertTrue(p.get("ownership_form_id").isNull(), "empty 1C reference becomes null");
        assertFalse(p.get("deleted").asBoolean());
        assertEquals("2026-07-10T12:00:00Z", p.get("updated_at").asText());
    }

    @Test
    void emptyOneCDateIsNull() throws Exception {
        JsonNode row = mapper.readTree("""
                {"Ref_Key":"11111111-1111-1111-1111-111111111111","Description":"ООО",
                 "DeletionMark":true,"ДатаИзменения":"0001-01-01T00:00:00"}""");

        ObjectNode p = Catalog.OWNERSHIP_FORMS.toPayload(row);

        assertTrue(p.get("updated_at").isNull());
        assertTrue(p.get("deleted").asBoolean());
    }

    @Test
    void rejectsRecordWithoutKey() throws Exception {
        JsonNode row = mapper.readTree("{\"Description\":\"x\"}");
        assertThrows(IllegalArgumentException.class, () -> Catalog.OWNERSHIP_FORMS.toPayload(row));
    }
}
