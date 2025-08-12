#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera um relatório Markdown do schema de um banco SQLite:
- Tabelas/Views e CREATE SQL
- Colunas (PRAGMA table_xinfo / table_info)
- Chaves estrangeiras (PRAGMA foreign_key_list)
- Índices (PRAGMA index_list + index_xinfo)
- Triggers
- Contagem de linhas (para tabelas)

Uso:
  python dump_schema_md.py --db db/atestmed.db --out schema.md
  python dump_schema_md.py --db db/atestmed.db          # imprime no stdout
"""

import argparse
import os
import sqlite3
from datetime import datetime
from textwrap import indent

def md_escape(val) -> str:
    if val is None:
        return ""
    s = str(val)
    # escapa pipes de tabelas Markdown
    return s.replace("|", r"\|")

def fetchone(conn, sql, params=()):
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    cur.close()
    return row

def fetchall(conn, sql, params=()):
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return rows

def get_tables_and_views(conn):
    rows = fetchall(conn, """
        SELECT type, name, tbl_name, sql
        FROM sqlite_master
        WHERE type IN ('table','view')
          AND name NOT LIKE 'sqlite_%'
        ORDER BY type DESC, name;
    """)
    # retorna lista de dicts
    return [{"type": r[0], "name": r[1], "tbl_name": r[2], "sql": r[3]} for r in rows]

def get_triggers_for(conn, tbl_name):
    rows = fetchall(conn, """
        SELECT name, sql
        FROM sqlite_master
        WHERE type='trigger' AND tbl_name=? AND name NOT LIKE 'sqlite_%'
        ORDER BY name;
    """, (tbl_name,))
    return [{"name": r[0], "sql": r[1]} for r in rows]

def get_table_columns(conn, tbl_name):
    """
    Usa PRAGMA table_xinfo se disponível (colunas ocultas), senão table_info.
    Retorna lista de dicts com: cid, name, type, notnull, dflt_value, pk, hidden?, generated?
    """
    cols = []
    try:
        rows = fetchall(conn, f"PRAGMA table_xinfo('{tbl_name}');")
        # table_xinfo: cid, name, type, notnull, dflt_value, pk, hidden
        if rows:
            for r in rows:
                d = {
                    "cid": r[0], "name": r[1], "type": r[2],
                    "notnull": r[3], "dflt_value": r[4], "pk": r[5],
                    "hidden": r[6] if len(r) > 6 else 0,
                    "generated": ""  # SQLite não expõe diretamente; deixamos vazio
                }
                cols.append(d)
            return cols
    except sqlite3.DatabaseError:
        pass

    # fallback: table_info
    rows = fetchall(conn, f"PRAGMA table_info('{tbl_name}');")
    for r in rows:
        d = {
            "cid": r[0], "name": r[1], "type": r[2],
            "notnull": r[3], "dflt_value": r[4], "pk": r[5],
            "hidden": 0, "generated": ""
        }
        cols.append(d)
    return cols

def get_foreign_keys(conn, tbl_name):
    rows = fetchall(conn, f"PRAGMA foreign_key_list('{tbl_name}');")
    # columns: id, seq, table, from, to, on_update, on_delete, match
    fks = []
    for r in rows:
        fks.append({
            "id": r[0], "seq": r[1], "table": r[2],
            "from": r[3], "to": r[4],
            "on_update": r[5], "on_delete": r[6], "match": r[7]
        })
    return fks

def get_indexes(conn, tbl_name):
    lst = fetchall(conn, f"PRAGMA index_list('{tbl_name}');")
    # columns: seq, name, unique, origin, partial
    idxs = []
    for seq, name, unique, origin, partial in lst:
        # index_xinfo (melhor que index_info para colunas geradas/expressões)
        try:
            xi = fetchall(conn, f"PRAGMA index_xinfo('{name}');")
            # columns: seqno, cid, name, desc, coll, key, origin, partial -> (varia por versão)
            cols = []
            for row in xi:
                # row[2] = column name (None se expressão)
                cols.append(row[2] if len(row) > 2 else None)
        except sqlite3.DatabaseError:
            ii = fetchall(conn, f"PRAGMA index_info('{name}');")
            cols = [row[2] for row in ii]  # seqno, cid, name
        idxs.append({
            "name": name,
            "unique": bool(unique),
            "origin": origin,
            "partial": bool(partial),
            "columns": cols
        })
    return idxs

def get_rowcount(conn, tbl_name):
    try:
        row = fetchone(conn, f'SELECT COUNT(*) FROM "{tbl_name}";')
        return int(row[0]) if row else None
    except sqlite3.DatabaseError:
        return None  # views podem falhar aqui

def render_table_md(conn, entry) -> str:
    t = entry["type"]
    name = entry["name"]
    sql = entry["sql"] or ""
    md = []

    rc = get_rowcount(conn, name) if t == "table" else None
    header = f"## {name} ({t})"
    if rc is not None:
        header += f" — {rc} linha(s)"
    md.append(header)
    md.append("")

    if sql.strip():
        md.append("```sql")
        md.append(sql.strip())
        md.append("```")
        md.append("")

    # Colunas
    cols = get_table_columns(conn, name)
    if cols:
        md.append("| cid | coluna | tipo | notnull | default | pk | hidden | generated |")
        md.append("|----:|--------|------|:-------:|---------|:--:|:------:|-----------|")
        for c in cols:
            md.append(
                f"| {c['cid']} "
                f"| {md_escape(c['name'])} "
                f"| {md_escape(c['type'])} "
                f"| {'✓' if c['notnull'] else ''} "
                f"| {md_escape(c['dflt_value'])} "
                f"| {'✓' if c['pk'] else ''} "
                f"| {'✓' if c.get('hidden') else ''} "
                f"| {md_escape(c.get('generated',''))} |"
            )
        md.append("")

    # FKs
    fks = get_foreign_keys(conn, name) if t == "table" else []
    if fks:
        md.append("**Chaves estrangeiras**")
        for fk in fks:
            md.append(
                f"- `{fk['from']}` → `{fk['table']}`(`{fk['to']}`)"
                f"  ON UPDATE {fk['on_update']}  ON DELETE {fk['on_delete']}"
                + (f"  MATCH {fk['match']}" if fk.get('match') else "")
            )
        md.append("")

    # Índices
    idxs = get_indexes(conn, name) if t == "table" else []
    if idxs:
        md.append("**Índices**")
        for ix in idxs:
            cols = ", ".join([f"`{c}`" if c else "(expr)" for c in ix["columns"]]) or "(sem colunas)"
            md.append(f"- { '`UNIQUE` ' if ix['unique'] else ''}`{ix['name']}`  →  ({cols})")
        md.append("")

    # Triggers
    trigs = get_triggers_for(conn, name)
    if trigs:
        md.append("**Triggers**")
        for tg in trigs:
            md.append(f"- `{tg['name']}`")
        md.append("")
        for tg in trigs:
            if tg["sql"]:
                md.append(f"```sql\n{tg['sql'].strip()}\n```")
                md.append("")

    return "\n".join(md)

def build_report(db_path: str) -> str:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")

    # Metadados
    sqlite_ver = fetchone(conn, "SELECT sqlite_version();")[0]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    entries = get_tables_and_views(conn)

    # Sumário (lista de tabelas/views)
    lines = []
    lines.append(f"# SQLite Schema Report")
    lines.append("")
    lines.append(f"- **Arquivo**: `{db_path}`")
    lines.append(f"- **SQLite**: {sqlite_ver}")
    lines.append(f"- **Gerado em**: {now}")
    lines.append("")
    if entries:
        lines.append("## Sumário")
        for e in entries:
            rc = get_rowcount(conn, e["name"]) if e["type"] == "table" else None
            extra = f" — {rc} linha(s)" if rc is not None else ""
            lines.append(f"- [{e['name']}](#{e['name'].lower()}) ({e['type']}){extra}")
        lines.append("")

    # Detalhes por tabela/view
    for e in entries:
        lines.append(render_table_md(conn, e))
        lines.append("")

    conn.close()
    return "\n".join(lines).strip() + "\n"

def main():
    ap = argparse.ArgumentParser(description="Extrai o schema do SQLite e gera Markdown.")
    ap.add_argument("--db", required=True, help="Caminho para o arquivo .db (ex.: db/atestmed.db)")
    ap.add_argument("--out", help="Arquivo de saída .md (se omitido, imprime no stdout)")
    args = ap.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"ERRO: banco não encontrado: {args.db}")

    md = build_report(args.db)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(md)
        print(f"✅ Relatório salvo em: {args.out}")
    else:
        print(md)

if __name__ == "__main__":
    main()

