#!/usr/bin/env bash
# Возвращает демо-стенд в исходное состояние:
#   1. данные 1С — ИНН/КПП тестовых контрагентов как при начальном заполнении, пометки удаления сняты
#      (через стандартный OData; при эмуляторе шаг пропускается — он заполняется заново при старте);
#   2. Kafka, PostgreSQL и метки синхронизации — пустые (docker compose down -v / up -d).
#
#   bash scripts/demo-reset.sh
#
# ODATA переопределяет адрес OData 1С (по умолчанию — публикация /demo1c на localhost).
set -euo pipefail
cd "$(dirname "$0")/.."

ODATA=${ODATA:-http://localhost/demo1c/odata/standard.odata}
COUNTERPARTIES="Catalog_%D0%9A%D0%BE%D0%BD%D1%82%D1%80%D0%B0%D0%B3%D0%B5%D0%BD%D1%82%D1%8B"
FORMS="Catalog_%D0%A4%D0%BE%D1%80%D0%BC%D1%8B%D0%A1%D0%BE%D0%B1%D1%81%D1%82%D0%B2%D0%B5%D0%BD%D0%BD%D0%BE%D1%81%D1%82%D0%B8"

# Наименование|ИНН|КПП — как в ДемоСервер.ЗаполнитьДемоДанные
SEED='ООО «Ромашка»|7701234567|770101001
ИП Иванов И.И.|500100732259|
АО «Север»|7802345678|780201001
ПАО «Восток»|2501234567|250101001
ООО «Лютик»|7703456789|770301001
ИП Петрова А.С.|771234567890|'

# Объекты набора по одному на строку
rows() {
  curl -fsS "$ODATA/$1?\$format=json" | tr -d '\n' | sed 's/},{/}\n{/g'
}

field() { # field <json-объект> <имя> -> значение (строка или true/false)
  printf '%s' "$1" | sed -n "s/.*\"$2\": *\"\\{0,1\\}\\([^\",}]*\\)\"\\{0,1\\}.*/\\1/p" | head -1
}

patch() { # patch <набор> <Ref_Key> <json>
  printf '%s' "$3" | curl -fsS -o /dev/null -X PATCH "$ODATA/$1(guid'$2')?\$format=json" \
       -H 'Content-Type: application/json' --data-binary @-
}

reset_1c() {
  echo "== 1С: возврат тестовых данных ($ODATA)"
  local all changed=0
  all=$(rows "$COUNTERPARTIES")
  while IFS='|' read -r name inn kpp; do
    obj=$(printf '%s\n' "$all" | grep -F "\"Description\": \"$name\"" || true)
    if [ -z "$obj" ]; then echo "   не найден: $name (пропущен)"; continue; fi
    key=$(field "$obj" Ref_Key)
    if [ "$(field "$obj" ИНН)" != "$inn" ] || [ "$(field "$obj" КПП)" != "$kpp" ] \
       || [ "$(field "$obj" DeletionMark)" != "false" ]; then
      patch "$COUNTERPARTIES" "$key" "{\"ИНН\":\"$inn\",\"КПП\":\"$kpp\",\"DeletionMark\":false}"
      echo "   восстановлен: $name"; changed=$((changed + 1))
    fi
  done <<< "$SEED"
  # формы собственности: только снять пометки удаления
  while read -r obj; do
    [ -n "$obj" ] || continue
    if [ "$(field "$obj" DeletionMark)" = "true" ]; then
      patch "$FORMS" "$(field "$obj" Ref_Key)" '{"DeletionMark":false}'
      echo "   снята пометка удаления: $(field "$obj" Description)"; changed=$((changed + 1))
    fi
  done <<< "$(rows "$FORMS")"
  echo "   изменено объектов: $changed"
}

if grep -qE '^COMPOSE_PROFILES=.*mock' .env 2>/dev/null; then
  echo "== 1С: режим эмулятора — данные будут заполнены заново при перезапуске"
else
  reset_1c
fi

echo "== Kafka, PostgreSQL, метки синхронизации: очистка"
docker compose down -v
docker compose up -d

echo "== Ожидание готовности сервисов"
for _ in $(seq 1 60); do
  status=$(docker compose ps --format '{{.Service}}={{.Health}}')
  if grep -q 'integration-service=healthy' <<< "$status" && grep -q 'consumer-service=healthy' <<< "$status"; then
    echo "Готово: стенд в исходном состоянии, PostgreSQL пуст. Дальше — sync full."
    exit 0
  fi
  sleep 5
done
echo "Сервисы не стали healthy за 5 минут: docker compose ps" >&2
exit 1
