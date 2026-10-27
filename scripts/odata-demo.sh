#!/usr/bin/env sh
# Изменение данных 1С через стандартный OData (PATCH) — шаги демо-сценария.
# Работает и с эмулятором onec-mock, и с реальной опубликованной 1С: запись идёт через объект 1С,
# поэтому модуль объекта проставляет ДатаИзменения так же, как при правке в интерфейсе.
#
#   scripts/odata-demo.sh list       # контрагенты
#   scripts/odata-demo.sh update     # ООО «Ромашка»: КПП 770101001 <-> 770101002
#   scripts/odata-demo.sh delete     # ООО «Лютик»: пометка удаления
#   scripts/odata-demo.sh restore    # ООО «Лютик»: снять пометку удаления
#
# ODATA — адрес OData (по умолчанию эмулятор). Для реальной 1С:
#   ODATA=http://localhost/demo1c/odata/standard.odata scripts/odata-demo.sh update
set -eu

ODATA=${ODATA:-http://localhost:8085/demo1c/odata/standard.odata}
# Catalog_Контрагенты, URL-encoded
SET="Catalog_%D0%9A%D0%BE%D0%BD%D1%82%D1%80%D0%B0%D0%B3%D0%B5%D0%BD%D1%82%D1%8B"

list() {
  curl -fsS "$ODATA/$SET?\$format=json&\$select=Ref_Key,Code,Description,%D0%9A%D0%9F%D0%9F,DeletionMark"
}

# Ref_Key контрагента по наименованию (наименования в демо-данных уникальны)
key_by_name() {
  list | tr -d '\n' | sed 's/},{/}\n{/g' | grep -F "\"$1\"" \
    | sed -n 's/.*"Ref_Key": *"\([0-9a-f-]\{36\}\)".*/\1/p' | head -1
}

patch() {
  key=$(key_by_name "$1")
  [ -n "$key" ] || { echo "not found: $1" >&2; exit 1; }
  # Тело передаётся через stdin: так кириллица не портится кодировкой командной строки Windows
  printf '%s' "$2" | curl -fsS -X PATCH "$ODATA/$SET(guid'$key')?\$format=json" \
       -H 'Content-Type: application/json' --data-binary @-
  echo
}

case "${1:-}" in
  list) list; echo ;;
  update)
    if list | tr -d '\n' | sed 's/},{/}\n{/g' | grep -F 'Ромашка' | grep -q '770101001'; then
      patch 'ООО «Ромашка»' '{"КПП":"770101002"}'
    else
      patch 'ООО «Ромашка»' '{"КПП":"770101001"}'
    fi ;;
  delete)  patch 'ООО «Лютик»' '{"DeletionMark":true}' ;;
  restore) patch 'ООО «Лютик»' '{"DeletionMark":false}' ;;
  *) echo "usage: $0 list|update|delete|restore" >&2; exit 2 ;;
esac
