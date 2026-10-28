# Интеграция 1С → Kafka → PostgreSQL

```
1С (OData) → integration-service (Java) → Kafka (1c.ownership_forms.v1, 1c.counterparties.v1) → consumer-service (Java) → PostgreSQL
```

* **1С** — конфигурация «ИнтеграцияДемо» со справочниками «Формы собственности» и «Контрагенты»,
  опубликованная через стандартный интерфейс OData.
* **integration-service** — Java 21. Читает справочники по OData, приводит записи к единому JSON-формату
  и публикует события в Kafka. Умеет полную и инкрементальную синхронизацию.
* **consumer-service** — Java 21. Читает события как consumer group и пакетами, транзакционно пишет их
  в PostgreSQL через `INSERT … ON CONFLICT DO UPDATE`. Для сбойных сообщений есть DLQ.
* Инфраструктура в Docker Compose: Kafka (KRaft), kafka-ui, PostgreSQL, оба сервиса и, при необходимости,
  эмулятор OData 1С.

Структура репозитория:

```
docker-compose.yml  .env.example  Makefile
integration-service/   Java: OData → Kafka
consumer-service/      Java: Kafka → PostgreSQL
migrations/            Flyway-миграции (применяет consumer-service)
sql/checks.sql         запросы для проверки данных
scripts/1c.ps1         создание/публикация базы 1С и демо-команды
scripts/odata-demo.sh     изменение данных через OData PATCH (эмулятор или реальная 1С)
1c/                    конфигурация (src/ XML, configuration.cf, configuration.dt), setup.md, default.vrd, odata-mock/
docs/architecture.md   архитектурные решения
```

## 1. Подготовка 1С

Подробно — в [1c/setup.md](1c/setup.md). Кратко:

```powershell
.\scripts\1c.ps1 create     # база 1c\ib: конфигурация из 1c\src + состав OData + тестовые данные
.\scripts\1c.ps1 publish    # публикация на Apache 2.4 как /demo1c (нужны модули веб-сервера 1С)
```

Проверка: `http://localhost/demo1c/odata/standard.odata/Catalog_Контрагенты?$format=json`.

Справочники:

| 1С | OData | PostgreSQL | Топик |
|---|---|---|---|
| ФормыСобственности | `Catalog_ФормыСобственности` | `ownership_forms` | `1c.ownership_forms.v1` |
| Контрагенты | `Catalog_Контрагенты` | `counterparties` | `1c.counterparties.v1` |

Тестовые данные: формы ООО, ИП, АО, ПАО; контрагенты ООО «Ромашка», ИП Иванов И.И., АО «Север»,
ПАО «Восток», ООО «Лютик», ИП Петрова А.С.

> **Без веб-сервера 1С.** Если модули расширения веб-сервера поставить нельзя, включите эмулятор OData.
> Он отдаёт те же наборы и поля, что и опубликованная конфигурация:
> ```
> COMPOSE_PROFILES=mock
> ONEC_ODATA_URL=http://onec-mock:8080/demo1c/odata/standard.odata
> ```

## 2. Запуск инфраструктуры

```bash
cp .env.example .env        # при необходимости поправьте ONEC_ODATA_URL и пароли
docker compose up -d --build
docker compose ps           # integration-service и consumer-service должны быть healthy
```

| Что | Адрес |
|---|---|
| Kafka UI | http://localhost:8090 |
| integration-service health/metrics, запуск синхронизации | http://localhost:8081/health, `/metrics`, `POST /sync` |
| consumer-service health/metrics | http://localhost:8082/health, `/metrics` |
| PostgreSQL | `localhost:5432`, логин и пароль из `.env` |
| Kafka с хоста | `localhost:9094` |

Секреты и адреса берутся из переменных окружения (`.env`), в коде их нет.

## 3. Полная синхронизация

```bash
docker compose exec integration-service ./sync --mode full      # или: make sync-full
```

Команда читает все записи обоих справочников (сначала формы собственности, затем контрагентов),
публикует их в Kafka и выводит отчёт: сколько записей прочитано, опубликовано и пропущено, и новую метку.

## 4. Инкрементальная синхронизация

```bash
docker compose exec integration-service ./sync --mode incremental   # или: make sync-incremental
```

Выгружаются только записи с `ДатаИзменения >= последняя метка − SYNC_OVERLAP_SECONDS`.
`ДатаИзменения` (UTC) проставляет модуль объекта 1С при каждой записи, в том числе при установке
пометки удаления. Метка хранится по каждому справочнику в томе `sync-state` (`/data/sync-state.json`)
и сдвигается только после того, как Kafka подтвердила доставку.

Перекрытие окна нужно, чтобы не потерять запись, закоммиченную в 1С в ту же секунду, но позже чтения.
Чтобы оно не отправляло одно и то же каждый раз, вместе с меткой сохраняются версии записей на границе
окна (`Ref_Key → DataVersion`): обычный инкремент такие записи читает, но повторно не публикует — в отчёте
они видны как `unchanged`. Изменённая запись получает в 1С новую `DataVersion` и уходит как обычно.
`--mode full` и явный `--since` публикуют всё.

Дополнительно:

```bash
./sync --mode incremental --since 2026-07-10T12:00:00     # явная метка (UTC)
./sync --mode full --entity counterparties                 # только один справочник
./sync --mode full --reset-state                           # сбросить метки и выгрузить всё заново
```

Чтобы синхронизация шла автоматически, задайте `SYNC_INTERVAL_SECONDS=60`: сервис будет запускать
инкрементальную выгрузку с этим интервалом.

**Запуск по HTTP.** Те же режимы доступны без `docker compose exec` — ручкой integration-service
(удобно из HTTP Client IDEA, `demo/demo.http`, или для вызова из самой 1С после записи объекта):

```bash
curl -X POST 'http://localhost:8081/sync?mode=full'                         # или: make api-sync-full
curl -X POST 'http://localhost:8081/sync?mode=incremental'                  # или: make api-sync-incremental
curl -X POST 'http://localhost:8081/sync?mode=full&entity=counterparties&reset=true'
curl -X POST 'http://localhost:8081/sync?mode=incremental&since=2026-07-10T12:00:00'
```

Запрос синхронный: ответ `200` приходит после подтверждения Kafka и содержит тот же отчёт, что и CLI.
`400` — неверные параметры, `409` — синхронизация уже идёт (расписание, CLI и HTTP делят одну
блокировку), `500` — ошибка синхронизации (метка не сдвигается, запрос можно повторить).
Если задан `SYNC_API_TOKEN`, нужен заголовок `Authorization: Bearer <токен>`; пустое значение
отключает проверку и годится только для локального стенда.

**Перезапуск синхронизации.** Все команды безопасно повторять: запись в PostgreSQL идемпотентна.
После сбоя достаточно запустить ту же команду ещё раз. Если метки потеряны или сомнительны —
`./sync --mode full --reset-state`. Чтобы заново залить PostgreSQL из Kafka, сбросьте offset группы:

```bash
docker compose stop consumer-service
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group pg-sink --reset-offsets --to-earliest --all-topics --execute
docker compose start consumer-service
```

## 5. Проверка сообщений в Kafka

* Kafka UI: http://localhost:8090 → Topics
  (скриншоты: [топики](docs/screenshots/kafka-ui-topics.png), [сообщения](docs/screenshots/kafka-ui-counterparties-messages.png)).
* Из консоли:
  ```bash
  make events-counterparties
  # или
  docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
    --topic 1c.counterparties.v1 --from-beginning --property print.key=true --timeout-ms 5000
  make dlq      # dead-letter topics с заголовками ошибки
  ```

> В Git Bash на Windows перед командами `docker compose exec kafka /opt/...` выполните
> `export MSYS_NO_PATHCONV=1`, иначе Git Bash подменит путь `/opt/...`.

Пример события:

```json
{
  "event_id": "fa296f0a-ad30-36fd-a032-460c536dbdf2",
  "event_type": "counterparty.upsert",
  "source": "1c",
  "occurred_at": "2026-10-27T08:16:33.297Z",
  "payload": {
    "id": "b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0005",
    "code": "000000005",
    "name": "ООО «Лютик»",
    "inn": "7703456789",
    "kpp": "770301001",
    "ownership_form_id": "8f0a7a10-0000-4000-8000-000000000001",
    "deleted": false,
    "updated_at": "2026-10-27T08:16:31Z",
    "data_version": "NTNkODAyYjQ="
  }
}
```

Ключ сообщения — `payload.id` (Ref_Key объекта 1С). Для форм собственности тип события
`ownership_form.upsert`, в payload нет полей `inn`, `kpp`, `ownership_form_id`. Удаление передаётся тем же
событием upsert с `deleted: true` (мягкое удаление).

## 6. Проверка данных в PostgreSQL

```bash
make db-check
# или
docker compose exec postgres psql -U integration -d integration -f /sql/checks.sql
```

[sql/checks.sql](sql/checks.sql) выводит обе таблицы, проверяет отсутствие дубликатов и считает итоги.

## 7. Демонстрационный сценарий

| Шаг | Реальная 1С | Эмулятор |
|---|---|---|
| ≥3 форм, ≥5 контрагентов | `.\scripts\1c.ps1 create` (4 и 6) | заполнено при старте (4 и 6) |
| Полная синхронизация | `make sync-full` | `make sync-full` |
| События в Kafka | `make events-counterparties` / Kafka UI | то же |
| Записи в PostgreSQL | `make db-check` | то же |
| Повторная полная синхронизация — без дублей | `make sync-full` → `make db-check` | то же |
| Изменить контрагента | `.\scripts\1c.ps1 demo-update`, вручную в 1С или `ODATA=http://localhost/demo1c/odata/standard.odata sh scripts/odata-demo.sh update` | `sh scripts/odata-demo.sh update` |
| Инкрементальная синхронизация | `make sync-incremental` | то же |
| Запись обновилась | `make db-check` (КПП «Ромашки», `imported_at`) | то же |
| Пометить на удаление | `.\scripts\1c.ps1 demo-delete`, F8/Del в 1С или `… scripts/odata-demo.sh delete` | `sh scripts/odata-demo.sh delete` |
| `deleted = true` | `make sync-incremental` → `make db-check` | то же |

**Повторить демо с начала:** `make demo-reset` (или `bash scripts/demo-reset.sh`). Скрипт возвращает
тестовым контрагентам в 1С исходные ИНН и КПП, снимает пометки удаления и пересоздаёт Kafka, PostgreSQL
и метки синхронизации с нуля. В IntelliJ IDEA то же делает конфигурация запуска `0. reset demo` из `.run/`;
запросы демо-сценария для HTTP Client лежат в [demo/demo.http](demo/demo.http).

`scripts/odata-demo.sh` меняет данные через PATCH стандартного OData 1С. Запись идёт через объект 1С,
поэтому `ПередЗаписью` проставляет `ДатаИзменения` так же, как при правке в интерфейсе. Пакетные команды
`.\scripts\1c.ps1 demo-*` должны запускаться той же версией платформы, что и веб-сервер.
Скрипт выбирает самую новую установленную платформу (см. [1c/setup.md](1c/setup.md)).

Сценарий прогонялся целиком на **реальной 1С** (8.3.27, Apache 2.4, файловая база) и на эмуляторе.
Результат:
* после полной синхронизации в БД 4 формы и 6 контрагентов;
* повторная полная синхронизация не создала ни одной новой строки, `imported_at` не изменился
  (`rows_changed: 0` в логе consumer-service);
* после изменения КПП в 1С инкрементальная синхронизация обновила только «ООО «Ромашка»»
  (`source_updated_at` = новая `ДатаИзменения` из 1С);
* после пометки удаления у «ООО «Лютик»» стало `deleted = t`;
* на эмуляторе дополнительно проверено: битое сообщение ушло в `1c.counterparties.dlq` с заголовком
  `dlq.reason`; при остановленном PostgreSQL consumer повторял запись с backoff (`/health` = 503), а после
  старта БД дописал изменение без потерь.

## 8. Архитектурные решения

Подробнее — в [docs/architecture.md](docs/architecture.md).

* **Стандартный OData 1С, а не свой HTTP-сервис.** Не нужен код интеграции на стороне 1С: публикация
  справочника — это настройка, а не разработка. Свою логику на стороне 1С добавляет только реквизит
  `ДатаИзменения` в модуле объекта.
* **Метка изменения в данных 1С.** Инкрементальная выгрузка фильтрует по `ДатаИзменения` (UTC из 1С)
  с небольшим перекрытием окна. Дубли на границе окна безопасны, т.к. запись в БД идемпотентна.
* **At-least-once + идемпотентный sink.** Producer работает с `acks=all` и идемпотентностью, consumer
  коммитит offset только после транзакции в БД. Upsert защищён от устаревших версий (сравнение
  `source_updated_at`) и не трогает строку, если данные не изменились.
* **Ключ сообщения = Ref_Key**, топики с compaction: порядок версий одного объекта сохраняется, а топик
  хранит актуальный снимок справочника.
* **Ссылочная целостность при двух топиках.** Если контрагент пришёл раньше своей формы
  собственности, создаётся строка-заглушка; настоящее событие формы её перезаписывает.
* **Разделение ошибок:** невалидные сообщения и ошибки данных уходят в DLQ, недоступность БД ведёт к
  повтору без коммита offset.
* **Минимум фреймворков:** чистая Java 21 (`java.net.http`, `kafka-clients`, JDBC/Hikari, Flyway),
  поэтому сервисы маленькие и быстро стартуют.

## 9. Ограничения

* **Инкремент по метке времени** не видит физически удалённые в 1С объекты: в OData их просто нет.
  Поэтому поддерживается только пометка удаления, как и требует задание. Точность метки — 1 секунда,
  и запись, закоммиченная в 1С позже своей `ДатаИзменения` более чем на `SYNC_OVERLAP_SECONDS`,
  может быть пропущена. Полная синхронизация это исправляет.
* Объекты, записанные до появления реквизита `ДатаИзменения` (пустая дата), попадают только в полную
  синхронизацию.
* Пагинация `$top/$skip` при одновременных изменениях в 1С может пропустить или повторить запись на
  границе страниц. Повтор безопасен, пропуск подберёт следующий запуск.
* Метка хранится в файле на томе одного экземпляра integration-service, горизонтального
  масштабирования нет.
* Kafka — один брокер без репликации, без TLS и SASL. PostgreSQL и OData — без TLS.
* Модули веб-сервера должны совпадать с версией и разрядностью платформы. Пока база открыта веб-сервером
  (8.3.27 x86), клиент другой версии к ней не подключится.

## 10. Что улучшить в промышленной версии

* **Планы обмена 1С** вместо метки времени. Регистрация изменений гарантирует доставку всех изменений,
  включая удаления, без окна перекрытия и без зависимости от времени коммита. Сервис читал бы изменения
  через HTTP-сервис 1С и подтверждал бы номер принятого сообщения.
* Схемы событий в Schema Registry (Avro/JSON Schema) с проверкой совместимости, версионирование топиков.
* Хранение меток в PostgreSQL или Kafka (outbox или compacted state topic), запуск по расписанию через
  оркестратор, распределённая блокировка.
* Kafka-кластер из 3+ брокеров, `min.insync.replicas=2`, TLS и SASL, ACL на топики.
* Метрики Micrometer/Prometheus, дашборды и алерты на лаг consumer group и рост DLQ, трассировка
  (OpenTelemetry) по `event_id`.
* Инструмент повторной обработки DLQ, интеграционные тесты на Testcontainers (Kafka + PostgreSQL),
  CI с прогоном демо-сценария.
* Аутентификация в OData отдельным пользователем 1С с ролью «только чтение» нужных справочников.
