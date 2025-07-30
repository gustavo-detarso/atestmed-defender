# 📊 ATESTMED - Scripts R para Análise Estatística

## 📁 Arquivo necessário

- `dados_analise.csv` (colocado na raiz ou pasta `r_stats`)
  - Deve conter as seguintes colunas:
    - `nomePerito`, `score_final`, `icra`, `iatd`, `prod`, `short_count`, `nc_ratio`, `cr`, `dr`, `grupo`

## 🚀 Execução recomendada

Use o script `run_all_rmd.R` para executar todos os `.Rmd` automaticamente:

```bash
Rscript run_all_rmd.R
```

> Este script renderiza todos os arquivos `.Rmd` da pasta `r_stats/scripts_rmd` com base nas datas definidas pelas variáveis de ambiente `DATA_START` e `DATA_END`.

Exemplo de uso com intervalo:

```bash
DATA_START=2025-01-01 DATA_END=2025-06-30 Rscript run_all_rmd.R
```

## 📦 Requisitos

Execute no terminal para instalar os pacotes:

```r
install.packages(c(
  "rmarkdown", "ggplot2", "dplyr", "readr", 
  "corrplot", "FactoMineR", "factoextra", "broom"
))
```

> Se estiver usando Linux, pode ser necessário executar com `sudo`:
> 
> ```bash
> sudo Rscript -e "install.packages(c(...))"
> ```

## 📂 Estrutura das saídas

Os resultados serão salvos na pasta `outputs/`:

| Tipo        | Arquivo                                 |
|-------------|------------------------------------------|
| Gráficos    | `outputs/*.png`                          |
| Tabelas     | `outputs/*.csv`                          |
| Modelos e testes | `outputs/*.txt`                    |
| Relatórios  | `outputs/*.html` (opcional via render)   |

## 🧪 Scripts incluídos

Todos os `.Rmd` estão na pasta `r_stats/scripts_rmd`:

- `estatisticas_basicas.Rmd`
- `teste_diferenca_grupos.Rmd`
- `modelo_regressao.Rmd`
- `pca_cluster.Rmd`
- `analise_cr_dr.Rmd`

Você pode também executar cada `.Rmd` individualmente com:

```bash
DATA_START=2025-01-01 DATA_END=2025-06-30 Rscript -e "rmarkdown::render('r_stats/scripts_rmd/estatisticas_basicas.Rmd', output_dir='outputs')"
```
