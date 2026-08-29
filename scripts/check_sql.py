#!/usr/bin/env python3
"""
Страж SQL-запросов.

Достаёт из Go-кода все SQL-литералы и прогоняет каждый через PREPARE на базе,
собранной из миграций проекта. Ловит то, что go build и go vet поймать не могут:

  * несуществующие таблицы и колонки;
  * синтаксические ошибки внутри строкового литерала;
  * неоднозначный вывод типов параметров (SQLSTATE 42P08) — ровно та ошибка,
    из-за которой 27.08.2026 были потеряны два подтверждённых платежа.

Запуск (нужен доступ к psql и права на создание временного кластера):

    scripts/check_sql.sh

Скрипт возвращает ненулевой код, если хотя бы один запрос не подготовился.
"""

import json
import os
import re
import subprocess
import sys
import tempfile

SQL_START = re.compile(r"^\s*(SELECT|INSERT|UPDATE|DELETE|WITH)\b", re.I | re.M)

PSQL = os.environ.get("CHECK_SQL_PSQL", "psql")
DSN = os.environ.get("CHECK_SQL_DSN", "")

# Запросы, которые собираются конкатенацией во время выполнения: подготовить их
# как есть нельзя. Проверяются отдельно, вручную, при изменении.
SKIP_DYNAMIC_MARKERS = ("%s", '" +')


def collect(root: str):
    """Собирает SQL-литералы из всех .go-файлов, кроме тестов."""
    out = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in (".git", "vendor", "node_modules")]
        for name in filenames:
            if not name.endswith(".go") or name.endswith("_test.go"):
                continue
            path = os.path.join(dirpath, name)
            src = open(path, encoding="utf-8", errors="replace").read()
            for m in re.finditer(r"`([^`]*)`", src, re.S):
                body = m.group(1)
                if not SQL_START.search(body):
                    continue
                line = src[: m.start()].count("\n") + 1
                dynamic = any(mark in body for mark in SKIP_DYNAMIC_MARKERS)
                head = src[max(0, m.start() - 40): m.start()]
                tail = src[m.end(): m.end() + 40]
                if re.search(r"\+\s*$", head) or re.match(r"\s*\+", tail):
                    dynamic = True
                rel = os.path.relpath(path, root)
                out.append({"file": rel, "line": line, "sql": body.strip(), "dynamic": dynamic})
    return out


def prepare(idx: int, sql: str) -> tuple[int, str]:
    """Пробует подготовить запрос. Возвращает (код возврата, вывод)."""
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
        f.write(f"PREPARE chk_{idx} AS\n{sql}\n;")
        tmp = f.name
    try:
        cmd = [PSQL, "-v", "ON_ERROR_STOP=1", "-q", "-f", tmp]
        if DSN:
            cmd = [PSQL, DSN, "-v", "ON_ERROR_STOP=1", "-q", "-f", tmp]
        p = subprocess.run(cmd, capture_output=True, text=True)
        return p.returncode, (p.stdout + p.stderr).strip()
    finally:
        os.unlink(tmp)


def main() -> int:
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    queries = collect(root)

    checked = 0
    skipped = []
    failures = []

    for i, q in enumerate(queries):
        if q["dynamic"]:
            skipped.append(q)
            continue
        checked += 1
        rc, out = prepare(i, q["sql"])
        if rc != 0:
            err = re.search(r"ERROR:.*", out)
            failures.append((q, err.group(0) if err else out[:300]))

    print(f"SQL-запросов найдено: {len(queries)}")
    print(f"проверено:           {checked}")
    print(f"пропущено (динамика): {len(skipped)}")
    for q in skipped:
        print(f"    ~ {q['file']}:{q['line']}")
    print(f"ошибок:              {len(failures)}")

    for q, err in failures:
        print()
        print(f"✗ {q['file']}:{q['line']}")
        print(f"   {err}")
        for line in [l for l in q["sql"].split("\n") if l.strip()][:4]:
            print(f"   | {line.strip()}")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())