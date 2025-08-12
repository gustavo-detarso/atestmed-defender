#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sqlite3, argparse
import pandas as pd

DB_PATH = os.path.join("db", "atestmed.db")

SQL_LONG = """
SELECT
  p.nomePerito,
  pr.siapePerito        AS matricula,
  prot.cr,
  prot.dr,
  prot.lotacao,
  pr.protocolo,
  pr.primeira_ini,
  pr.primeira_fim,
  pr.proximo_inicio,
  pr.n_sessoes,
  pr.n_reaberturas
FROM protocolos_reabertos pr
JOIN peritos     p    ON p.siapePerito = pr.siapePerito
LEFT JOIN protocolos prot ON prot.protocolo = pr.protocolo
WHERE 1=1
  AND (:start IS NULL OR date(pr.proximo_inicio) >= date(:start))
  AND (:end   IS NULL OR date(pr.proximo_inicio) <= date(:end))
ORDER BY p.nomePerito, pr.proximo_inicio;
"""

SQL_GROUP = """
SELECT
  p.nomePerito,
  pr.siapePerito        AS matricula,
  prot.cr,
  prot.dr,
  prot.lotacao,
  COUNT(*)              AS qtde_protocolos_reabertos,
  GROUP_CONCAT(DISTINCT pr.protocolo ORDER BY pr.proximo_inicio) AS protocolos
FROM protocolos_reabertos pr
JOIN peritos     p    ON p.siapePerito = pr.siapePerito
LEFT JOIN protocolos prot ON prot.protocolo = pr.protocolo
WHERE 1=1
  AND (:start IS NULL OR date(pr.proximo_inicio) >= date(:start))
  AND (:end   IS NULL OR date(pr.proximo_inicio) <= date(:end))
GROUP BY p.nomePerito, pr.siapePerito, prot.cr, prot.dr, prot.lotacao
ORDER BY qtde_protocolos_reabertos DESC, p.nomePerito;
"""

def main():
    ap = argparse.ArgumentParser(description="Lista protocolos reabertos no Brasil todo.")
    ap.add_argument("--start", help="YYYY-MM-DD (opcional)")
    ap.add_argument("--end", help="YYYY-MM-DD (opcional)")
    ap.add_argument("--grouped", action="store_true", help="Agrupar por perito/CR/DR/lotação")
    ap.add_argument("--export-csv", help="Arquivo CSV de saída (opcional)")
    args = ap.parse_args()

    if not os.path.exists(DB_PATH):
        raise SystemExit(f"Banco não encontrado em {DB_PATH}")

    sql = SQL_GROUP if args.grouped else SQL_LONG
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(sql, conn, params={"start": args.start, "end": args.end})

    if df.empty:
        print("Nenhum protocolo reaberto encontrado no período/escopo informado.")
        return

    pd.set_option("display.max_columns", None)
    print(df.to_string(index=False))

    if args.export_csv:
        os.makedirs(os.path.dirname(args.export_csv) or ".", exist_ok=True)
        df.to_csv(args.export_csv, index=False, encoding="utf-8")
        print(f"\n✅ Exportado: {args.export_csv}")

if __name__ == "__main__":
    main()

