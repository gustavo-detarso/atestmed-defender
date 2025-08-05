#!/usr/bin/env python3
import os
import subprocess
import datetime
import zipfile
import sqlite3
import pandas as pd

# Caminhos
ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_CSV = os.path.join(ROOT, "r_stats", "data", "dados_analise.csv")
DB_PATH = os.path.join(ROOT, "db", "atestmed.db")
OUTPUT_ROOT = os.path.join(ROOT, "r_stats", "outputs")
LOGS_DIR = os.path.join(ROOT, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOGS_DIR, f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}.log")

# Variáveis globais para período
START_DATE = ""
END_DATE = ""

def log(msg):
    with open(LOG_PATH, "a") as f:
        f.write(msg + "\n")
    print(msg)

def instalar_pacotes_r():
    log("📦 Instalando pacotes R...")
    script_r = '''
install_if_missing <- function(pkg) {
  if (!require(pkg, character.only = TRUE)) {
    message(sprintf("🔄 Instalando: %s", pkg))
    install.packages(pkg, dependencies = TRUE, repos = "https://cloud.r-project.org")
  } else {
    message(sprintf("✅ Já instalado: %s", pkg))
  }
}

pkgs <- c("rmarkdown", "ggplot2", "dplyr", "readr", "corrplot",
          "broom", "cluster", "rstatix", "ggpubr", "car", "quantreg", "SparseM")
for (p in pkgs) install_if_missing(p)

final_pkgs <- c("FactoMineR", "factoextra")
for (p in final_pkgs) install_if_missing(p)

if (!require("tinytex", character.only = TRUE)) {
  install.packages("tinytex", repos = "https://cloud.r-project.org")
}
if (!tinytex::is_tinytex()) {
  tinytex::install_tinytex()
}
'''
    subprocess.run(["Rscript", "-e", script_r], check=True)

def perguntar_periodo():
    global START_DATE, END_DATE
    escolha = input("📅 Usar todo o período do banco? (S/N): ").strip().lower()
    if escolha == "s":
        START_DATE, END_DATE = "", ""
    else:
        START_DATE = input("Digite a data inicial (YYYY-MM-DD): ").strip()
        END_DATE = input("Digite a data final   (YYYY-MM-DD): ").strip()

def gerar_csv():
    log("🧱 Gerando dados de análise a partir do banco...")
    os.makedirs(os.path.dirname(DATA_CSV), exist_ok=True)

    filtro_data = ""
    params = {}
    if START_DATE and END_DATE:
        filtro_data = "AND date(a.dataHoraIniPericia) BETWEEN :start AND :end"
        params["start"] = START_DATE
        params["end"] = END_DATE

    conn = sqlite3.connect(DB_PATH)
    query = f'''
        SELECT 
            p.nomePerito, 
            i.scoreFinal AS score_final,
            i.icra,
            i.iatd,
            ROUND(COUNT(a.protocolo) * 3600.0 / SUM((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400), 2) AS prod,
            SUM(CASE WHEN (julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400 <= 15 THEN 1 ELSE 0 END) AS short_count,
            SUM(CASE WHEN a.motivoNaoConformado = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(a.protocolo) AS nc_ratio,
            p.cr,
            p.dr,
            CASE 
                WHEN i.scoreFinal >= 6 THEN 'Top10'
                ELSE 'Outros'
            END AS grupo
        FROM analises a
        JOIN peritos p ON a.siapePerito = p.siapePerito
        JOIN indicadores i ON a.siapePerito = i.perito
        WHERE a.dataHoraIniPericia IS NOT NULL 
          AND a.dataHoraFimPericia IS NOT NULL
          {filtro_data}
        GROUP BY a.siapePerito
        HAVING COUNT(a.protocolo) >= 50
    '''
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    df.to_csv(DATA_CSV, index=False)
    log(f"✅ CSV salvo com sucesso em: {DATA_CSV}")

def renderizar_relatorios():
    log("🧪 Renderizando relatórios RMarkdown...")

    r_script = f'''
suppressMessages({{
  library(rmarkdown)
  library(glue)
  library(fs)
  library(tools)
}})

csv_path <- normalizePath("{DATA_CSV}")
if (!file.exists(csv_path)) stop(glue("❌ CSV não encontrado: {{csv_path}}"))

data_iso <- Sys.Date() |> format("%Y-%m-%d")
scripts_dir <- "r_stats/scripts_r"
output_root <- "r_stats/outputs"
formatos <- list(html = "html_document", pdf = "pdf_document", md = "md_document")

for (formato in names(formatos)) {{
  dir_create(file.path(output_root, formato, data_iso))
}}

scripts <- dir_ls(scripts_dir, glob = "*.Rmd")

for (script in scripts) {{
  script_nome <- file_path_sans_ext(path_file(script))
  for (formato in names(formatos)) {{
    output_base <- file.path(output_root, formato, data_iso)
    tryCatch({{
      render(
        input = script,
        output_format = formatos[[formato]],
        output_file = paste0(script_nome, ".", formato),
        output_dir = output_base,
        params = list(
          arquivo = csv_path,
          start_date = "{START_DATE if START_DATE else 'Período Completo'}",
          end_date = "{END_DATE if END_DATE else 'Período Completo'}"
        ),
        quiet = TRUE
      )
      cat("✅ Sucesso:", script_nome, "→", formato, "\\n\\n")
    }}, error = function(e) {{
      cat("❌ Erro ao renderizar", script_nome, "(", formato, "):", e$message, "\\n\\n")
    }})
  }}
}}
'''
    subprocess.run(["Rscript", "-e", r_script], check=True)

def compactar_outputs():
    data_iso = datetime.date.today().isoformat()
    zip_path = os.path.join(ROOT, f"rstats_outputs_{data_iso}.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(OUTPUT_ROOT):
            for file in files:
                fpath = os.path.join(root, file)
                zipf.write(fpath, os.path.relpath(fpath, ROOT))
    log(f"📦 Arquivo compactado salvo em: {zip_path}")

def menu():
    log(f"📄 Log em: {LOG_PATH}")
    print("🧭 Selecione a opção de execução:")
    print("[1] Instalar pacotes R e TinyTeX")
    print("[2] Gerar CSV a partir do banco")
    print("[3] Renderizar relatórios RMarkdown")
    print("[4] Executar tudo em sequência")
    print("[0] Sair")
    opcao = input("Digite a opção desejada: ").strip()

    if opcao == "1":
        instalar_pacotes_r()
    elif opcao == "2":
        perguntar_periodo()
        gerar_csv()
    elif opcao == "3":
        renderizar_relatorios()
        compactar_outputs()
    elif opcao == "4":
        instalar_pacotes_r()
        perguntar_periodo()
        gerar_csv()
        renderizar_relatorios()
        compactar_outputs()
    elif opcao == "0":
        print("Saindo.")
    else:
        print("Opção inválida.")

if __name__ == "__main__":
    menu()

