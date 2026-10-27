package ru.integration.sink.db;

import ru.integration.sink.model.CatalogEvent;
import ru.integration.sink.model.CatalogEvent.Counterparty;
import ru.integration.sink.model.CatalogEvent.OwnershipForm;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Идемпотентная запись справочников: INSERT ... ON CONFLICT DO UPDATE.
 * <ul>
 *   <li>повторное получение того же события ничего не меняет (IS DISTINCT FROM);</li>
 *   <li>более старая версия не затирает более новую (сравнение source_updated_at);</li>
 *   <li>пакет пишется в одной транзакции.</li>
 * </ul>
 */
public class CatalogRepository {

    private static final String UPSERT_OWNERSHIP_FORM = """
            INSERT INTO ownership_forms AS t
                (id, code, name, deleted, source_updated_at, source_data_version, last_event_id, placeholder)
            VALUES (?, ?, ?, ?, ?, ?, ?, false)
            ON CONFLICT (id) DO UPDATE SET
                code = EXCLUDED.code,
                name = EXCLUDED.name,
                deleted = EXCLUDED.deleted,
                source_updated_at = EXCLUDED.source_updated_at,
                source_data_version = EXCLUDED.source_data_version,
                last_event_id = EXCLUDED.last_event_id,
                placeholder = false,
                imported_at = now()
            WHERE (t.source_updated_at IS NULL OR EXCLUDED.source_updated_at IS NULL
                   OR EXCLUDED.source_updated_at >= t.source_updated_at)
              AND (t.code, t.name, t.deleted, t.source_updated_at, t.source_data_version, t.placeholder)
                  IS DISTINCT FROM
                  (EXCLUDED.code, EXCLUDED.name, EXCLUDED.deleted, EXCLUDED.source_updated_at,
                   EXCLUDED.source_data_version, false)
            """;

    /** Ссылка на ещё не загруженную форму собственности: создаём заглушку, её перезапишет настоящее событие. */
    private static final String INSERT_OWNERSHIP_FORM_PLACEHOLDER = """
            INSERT INTO ownership_forms (id, name, placeholder)
            VALUES (?, '(ожидает загрузки из 1С)', true)
            ON CONFLICT (id) DO NOTHING
            """;

    private static final String UPSERT_COUNTERPARTY = """
            INSERT INTO counterparties AS t
                (id, code, name, inn, kpp, ownership_form_id, deleted, source_updated_at, source_data_version, last_event_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                code = EXCLUDED.code,
                name = EXCLUDED.name,
                inn = EXCLUDED.inn,
                kpp = EXCLUDED.kpp,
                ownership_form_id = EXCLUDED.ownership_form_id,
                deleted = EXCLUDED.deleted,
                source_updated_at = EXCLUDED.source_updated_at,
                source_data_version = EXCLUDED.source_data_version,
                last_event_id = EXCLUDED.last_event_id,
                imported_at = now()
            WHERE (t.source_updated_at IS NULL OR EXCLUDED.source_updated_at IS NULL
                   OR EXCLUDED.source_updated_at >= t.source_updated_at)
              AND (t.code, t.name, t.inn, t.kpp, t.ownership_form_id, t.deleted, t.source_updated_at, t.source_data_version)
                  IS DISTINCT FROM
                  (EXCLUDED.code, EXCLUDED.name, EXCLUDED.inn, EXCLUDED.kpp, EXCLUDED.ownership_form_id,
                   EXCLUDED.deleted, EXCLUDED.source_updated_at, EXCLUDED.source_data_version)
            """;

    private final DataSource dataSource;

    public CatalogRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Записывает пакет в одной транзакции. Формы собственности пишутся раньше контрагентов,
     * чтобы внешний ключ counterparties.ownership_form_id был удовлетворён.
     *
     * @return число реально изменённых строк
     */
    public int write(List<? extends CatalogEvent> events) throws SQLException {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            try {
                int changed = writeOwnershipForms(c, events) + writeCounterparties(c, events);
                c.commit();
                return changed;
            } catch (SQLException | RuntimeException e) {
                c.rollback();
                throw e;
            }
        }
    }

    public boolean isAvailable() {
        try (Connection c = dataSource.getConnection()) {
            return c.isValid(2);
        } catch (SQLException e) {
            return false;
        }
    }

    private int writeOwnershipForms(Connection c, List<? extends CatalogEvent> events) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(UPSERT_OWNERSHIP_FORM)) {
            int n = 0;
            for (CatalogEvent e : events) {
                if (e instanceof OwnershipForm f) {
                    ps.setString(1, f.id());
                    ps.setString(2, f.code());
                    ps.setString(3, f.name());
                    ps.setBoolean(4, f.deleted());
                    ps.setObject(5, f.updatedAt());
                    ps.setString(6, f.dataVersion());
                    ps.setObject(7, f.eventId());
                    ps.addBatch();
                    n++;
                }
            }
            return n == 0 ? 0 : sum(ps.executeBatch());
        }
    }

    private int writeCounterparties(Connection c, List<? extends CatalogEvent> events) throws SQLException {
        Set<String> referencedForms = new LinkedHashSet<>();
        for (CatalogEvent e : events) {
            if (e instanceof Counterparty cp && cp.ownershipFormId() != null) {
                referencedForms.add(cp.ownershipFormId());
            }
        }
        if (!referencedForms.isEmpty()) {
            try (PreparedStatement ps = c.prepareStatement(INSERT_OWNERSHIP_FORM_PLACEHOLDER)) {
                for (String id : referencedForms) {
                    ps.setString(1, id);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
        try (PreparedStatement ps = c.prepareStatement(UPSERT_COUNTERPARTY)) {
            int n = 0;
            for (CatalogEvent e : events) {
                if (e instanceof Counterparty cp) {
                    ps.setObject(1, cp.id());
                    ps.setString(2, cp.code());
                    ps.setString(3, cp.name());
                    ps.setString(4, cp.inn());
                    ps.setString(5, cp.kpp());
                    ps.setString(6, cp.ownershipFormId());
                    ps.setBoolean(7, cp.deleted());
                    ps.setObject(8, cp.updatedAt());
                    ps.setString(9, cp.dataVersion());
                    ps.setObject(10, cp.eventId());
                    ps.addBatch();
                    n++;
                }
            }
            return n == 0 ? 0 : sum(ps.executeBatch());
        }
    }

    private static int sum(int[] counts) {
        int total = 0;
        for (int count : counts) {
            total += Math.max(count, 0);
        }
        return total;
    }
}
