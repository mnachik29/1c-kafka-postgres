-- Нормализованная структура справочников, выгружаемых из 1С.
-- Применяется consumer-service через Flyway при старте.

CREATE TABLE ownership_forms (
    id                  TEXT PRIMARY KEY,              -- Ref_Key объекта 1С
    code                TEXT,
    name                TEXT        NOT NULL,
    deleted             BOOLEAN     NOT NULL DEFAULT false,
    source_updated_at   TIMESTAMPTZ,                   -- ДатаИзменения из 1С (UTC)
    source_data_version TEXT,                          -- DataVersion объекта 1С
    last_event_id       UUID,
    placeholder         BOOLEAN     NOT NULL DEFAULT false, -- создана по ссылке контрагента до прихода самой формы
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(), -- первый импорт
    imported_at         TIMESTAMPTZ NOT NULL DEFAULT now()  -- последний импорт, изменивший запись
);

CREATE TABLE counterparties (
    id                  UUID PRIMARY KEY,              -- Ref_Key объекта 1С
    code                TEXT,
    name                TEXT        NOT NULL,
    inn                 TEXT,
    kpp                 TEXT,
    ownership_form_id   TEXT REFERENCES ownership_forms (id),
    deleted             BOOLEAN     NOT NULL DEFAULT false,
    source_updated_at   TIMESTAMPTZ,
    source_data_version TEXT,
    last_event_id       UUID,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    imported_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX counterparties_ownership_form_idx ON counterparties (ownership_form_id);
CREATE INDEX counterparties_inn_idx ON counterparties (inn);
