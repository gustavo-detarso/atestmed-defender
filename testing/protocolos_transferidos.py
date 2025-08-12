#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sqlite3, argparse, pandas as pd

DB = os.path.join("db", "atestmed.db")

CTE_STARTS = """
WITH
starts AS (
  SELECT a.protocolo, a.siapePerito AS perito_inicio, a.dataHoraIniPericia AS primeira_ini
  FROM analises a
  JOIN (
    SELECT protocolo, MIN(dataHoraIniPericia) AS primeira_ini
    FROM analises
    WHERE dataHoraIniPericia IS NOT NULL AND dataHoraIniPericia <> ''
    GROUP BY protocolo
  ) m ON m.protocolo = a.protocolo AND m.primeira_ini = a.dataHoraIniPericia
),
ends AS (
  SELECT a.protocolo, a.siapePerito AS perito_fim, a.dataHoraFimPericia AS ultima_fim
  FROM analises a
  JOIN (
    SELECT protocolo, MAX(dataHoraFimPericia) AS ultima_fim
    FROM analises
    WHERE dataHoraFimPericia IS NOT NULL AND dataHoraFimPericia <> ''
    GROUP BY protocolo
  ) m ON m.protocolo = a.protocolo AND m.ultima_fim = a.dataHoraFimPericia
)
"""

SQL_DET = CTE_STARTS + """
SELECT
  s.protocolo, s.primeira_ini, e.ultima_fim,
  s.perito_inicio AS siape_inicio, p1.nomePerito AS nome_inicio,
  e.perito_fim    AS siape_fim,    p2.nomePerito AS nome_fim,
  prot.cr, prot.dr, prot.lotacao
FROM starts s
JOIN ends   e    ON e.protocolo = s.protocolo
JOIN peritos p1  ON p1.siapePerito = s.perito_inicio
JOIN peritos p2  ON p2.siapePerito = e.perito_fim
LEFT JOIN protocolos prot ON prot.protocolo = s.protocolo
WHERE s.perito_inicio <> e.perito_fim
  AND (:start IS NULL OR date(s.primeira_ini) >= date(:start))
  AND (:end   IS NULL OR date(e.ultima_fim)   <= date(:end))
ORDER BY s.primeira_ini;
"""

SQL_GROUP = CTE_STARTS + """
SELECT
  p1.nomePerito AS quem_iniciou,
  s.perito_inicio AS siape_iniciou,
  p2.nomePerito AS quem_finalizou,
  e.perito_fim    AS siape_finalizou,
  prot.cr, prot.dr, prot.lotacao,
  COUNT(*) AS qtde_protocolos,
  GROUP_CONCAT(DISTINCT s.protocolo) AS protocolos
FROM starts s
JOIN ends   e    ON e.protocolo = s.protocolo
JOIN peritos p1  ON p1.siapePerito = s.perito_inicio
JOIN peritos p2  ON p2.siapePerito = e.perito_fim
LEFT JOIN protocolos prot ON prot.protocolo = s.protocolo
WHERE s.perito_inicio <> e.perito_fim
  AND (:start IS NULL OR date(s.primeira_ini) >= date(:start))
  AND (:end   IS NULL OR date(e.ultima_fim)   <= date(:end))
GROUP BY p1.nomePerito, s.perito_inicio, p2.nomePerito, e.perito_fim, prot.cr, prot.dr, prot.lotacao
ORDER BY qtde_protocolos DESC;
"""

def main():
    ap = argparse.ArgumentParser(description="Protocolos iniciados por um perito e concluídos por outro.")
    ap.add_argument("--start", help="YYYY-MM-DD (opcional)")
    ap.add_argument("--end", help="YYYY-MM-DD (opcional)")
    ap.add_argument("--grouped", action="store_true", help="Agrupar por par (quem começou x quem finalizou)")
    ap.add_argument("--export-csv", help="Arquivo CSV de saída (opcional)")
    args = ap.parse_args()

    if not os.path.exists(DB):
        raise SystemExit(f"Banco não encontrado em {DB}")

    sql = SQL_GROUP if args.grouped else SQL_DET
    with sqlite3.connect(DB) as conn:
        df = pd.read_sql_query(sql, conn, params={"start": args.start, "end": args.end})

    if df.empty:
        print("Nenhum protocolo transferido entre peritos encontrado no período.")
        return

    pd.set_option("display.max_columns", None)
    print(df.to_string(index=False))

    if args.export_csv:
        os.makedirs(os.path.dirname(args.export_csv) or ".", exist_ok=True)
        df.to_csv(args.export_csv, index=False, encoding="utf-8")
        print(f"\n✅ Exportado: {args.export_csv}")

if __name__ == "__main__":
    main()

