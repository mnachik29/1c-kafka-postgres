package ru.integration.sink.model;

import java.time.OffsetDateTime;
import java.util.UUID;

/** Разобранное и провалидированное событие справочника 1С. */
public sealed interface CatalogEvent {

    UUID eventId();

    String key();

    OffsetDateTime updatedAt();

    record OwnershipForm(UUID eventId, String id, String code, String name, boolean deleted,
                         OffsetDateTime updatedAt, String dataVersion) implements CatalogEvent {
        @Override
        public String key() {
            return id;
        }
    }

    record Counterparty(UUID eventId, UUID id, String code, String name, String inn, String kpp,
                        String ownershipFormId, boolean deleted, OffsetDateTime updatedAt,
                        String dataVersion) implements CatalogEvent {
        @Override
        public String key() {
            return id.toString();
        }
    }
}
