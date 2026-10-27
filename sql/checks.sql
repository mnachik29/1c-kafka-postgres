-- Проверка результата синхронизации.
-- make db-check  или  docker compose exec postgres psql -U integration -d integration -f /sql/checks.sql

\echo '--- Формы собственности'
SELECT id, code, name, deleted, placeholder, source_updated_at, imported_at
FROM ownership_forms
ORDER BY code;

\echo '--- Контрагенты'
SELECT c.code, c.name, c.inn, c.kpp, f.name AS ownership_form, c.deleted, c.source_updated_at, c.imported_at
FROM counterparties c
LEFT JOIN ownership_forms f ON f.id = c.ownership_form_id
ORDER BY c.code;

\echo '--- Контроль дубликатов (должно быть пусто)'
SELECT 'counterparties' AS tbl, inn, count(*) FROM counterparties GROUP BY inn HAVING count(*) > 1
UNION ALL
SELECT 'ownership_forms', name, count(*) FROM ownership_forms GROUP BY name HAVING count(*) > 1;

\echo '--- Итоги'
SELECT (SELECT count(*) FROM ownership_forms)                 AS ownership_forms,
       (SELECT count(*) FROM counterparties)                  AS counterparties,
       (SELECT count(*) FROM counterparties WHERE deleted)    AS counterparties_deleted,
       (SELECT count(*) FROM ownership_forms WHERE placeholder) AS pending_placeholders;
