#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(
    cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &&
    pwd -P
)"

APP="$ROOT/app.py"
REQ="$ROOT/requirements.txt"
ENV_EXAMPLE="$ROOT/.env.example"
UNIT_TEMPLATE="$ROOT/systemd/sondehub-plus.service.example"

UNIT="/etc/systemd/system/sondehub-plus.service"
ENV_FILE="/etc/default/sondehub-plus"

MODE="install"

if [ "${1:-}" = "--check" ]; then
    MODE="check"
elif [ -n "${1:-}" ]; then
    echo "Usage:"
    echo "  ./install.sh --check"
    echo "  sudo ./install.sh"
    exit 2
fi


echo
echo "============================================================"
echo " SondeHub+ installer"
echo " MODE=$MODE"
echo " ROOT=$ROOT"
echo "============================================================"


for f in \
    "$APP" \
    "$REQ" \
    "$ENV_EXAMPLE" \
    "$UNIT_TEMPLATE"
do
    if [ ! -f "$f" ]; then
        echo "BRAK: $f"
        exit 10
    fi
done


if [[ "$ROOT" =~ [[:space:]] ]]; then
    echo "BŁĄD: ścieżka repo zawiera spację."
    exit 11
fi


PYTHON="${PYTHON:-python3}"

if ! command -v "$PYTHON" >/dev/null 2>&1; then
    echo "BRAK python3"
    exit 12
fi


"$PYTHON" - "$APP" <<'PY'
import re
import sys
from pathlib import Path

p = Path(sys.argv[1])
src = p.read_text(encoding="utf-8")

compile(src, str(p), "exec")

for marker in (
    "SONDEHUB_PUBLIC_CONFIG_V32C",
    "SONDEHUB_PUBLIC_HOST_V34",
    "SONDEHUB_PUBLIC_CALLSIGN_V36C",
    "SONDEHUB_PUBLIC_WATCH_ENDPOINT_V38B",
    "SONDEHUB_PUBLIC_INLINE_JSON_V45",
):
    if marker not in src:
        raise SystemExit(
            "Brak markera: " + marker
        )

privacy_patterns = (
    (
        "local hostname",
        re.compile(
            r"(?i)"
            r"(?<![A-Za-z0-9_-])"
            r"[A-Za-z0-9]"
            r"(?:[A-Za-z0-9._-]*[A-Za-z0-9])?"
            r"\.local"
            r"(?![A-Za-z0-9_-])"
        ),
    ),
    (
        "absolute home path",
        re.compile(
            r"/(?:home|Users)/"
            r"[A-Za-z0-9._-]+"
        ),
    ),
)

for label, pattern in privacy_patterns:
    if pattern.search(src):
        raise SystemExit(
            "Potential private element in app.py: "
            + label
        )

print("APP_CHECK=OK")
PY


for marker in \
    '@@USER@@' \
    '@@GROUP@@' \
    '@@REPO@@'
do
    if ! grep -qF "$marker" "$UNIT_TEMPLATE"; then
        echo "BRAK PLACEHOLDERA: $marker"
        exit 13
    fi
done


if [ "$MODE" = "check" ]; then
    echo
    echo "INSTALLER_CHECK=OK"
    echo "NICZEGO NIE ZMIENIONO"
    exit 0
fi


if [ "$EUID" -ne 0 ]; then
    echo "Instalacja wymaga sudo."
    exit 20
fi


RUN_USER="${SONDEHUB_PLUS_USER:-${SUDO_USER:-}}"

if [ -z "$RUN_USER" ] || [ "$RUN_USER" = "root" ]; then
    echo "Nie ustalono użytkownika usługi."
    echo
    echo "Przykład:"
    echo "sudo SONDEHUB_PLUS_USER=pi ./install.sh"
    exit 21
fi


if ! id "$RUN_USER" >/dev/null 2>&1; then
    echo "Nie istnieje użytkownik: $RUN_USER"
    exit 22
fi


RUN_GROUP="$(
    id -gn "$RUN_USER"
)"


echo "USER =$RUN_USER"
echo "GROUP=$RUN_GROUP"
echo "ROOT =$ROOT"


echo
echo "===== VENV ====="

if [ ! -x "$ROOT/.venv/bin/python" ]; then

    runuser \
        -u "$RUN_USER" \
        -- \
        "$PYTHON" -m venv \
        "$ROOT/.venv"

fi


runuser \
    -u "$RUN_USER" \
    -- \
    "$ROOT/.venv/bin/python" \
    -m pip install \
    -r "$REQ"


echo
echo "===== CONFIG ====="

if [ ! -f "$ENV_FILE" ]; then

    install \
        -m 640 \
        -o root \
        -g "$RUN_GROUP" \
        "$ENV_EXAMPLE" \
        "$ENV_FILE"

    echo "Utworzono:"
    echo "$ENV_FILE"

else

    echo "Istniejący config pozostawiono:"
    echo "$ENV_FILE"

fi


echo
echo "===== SYSTEMD ====="

TMP="$(
    mktemp
)"

trap 'rm -f "$TMP"' EXIT


escape_sed() {
    printf '%s' "$1" |
    sed 's/[&|]/\\&/g'
}


U="$(
    escape_sed "$RUN_USER"
)"

G="$(
    escape_sed "$RUN_GROUP"
)"

R="$(
    escape_sed "$ROOT"
)"


sed \
    -e "s|@@USER@@|$U|g" \
    -e "s|@@GROUP@@|$G|g" \
    -e "s|@@REPO@@|$R|g" \
    "$UNIT_TEMPLATE" \
    > "$TMP"


if grep -q '@@' "$TMP"; then
    echo "BŁĄD: nierozwiązany placeholder."
    exit 30
fi


install \
    -m 644 \
    "$TMP" \
    "$UNIT"


if command -v systemd-analyze >/dev/null 2>&1; then

    systemd-analyze verify \
        "$UNIT"

fi


systemctl daemon-reload


echo
echo "============================================================"
echo " INSTALACJA PLIKÓW GOTOWA"
echo
echo " Usługa NIE została uruchomiona."
echo " Usługa NIE została włączona."
echo
echo " Edytuj:"
echo "   /etc/default/sondehub-plus"
echo
echo " Następnie:"
echo "   sudo systemctl enable --now sondehub-plus.service"
echo "============================================================"
