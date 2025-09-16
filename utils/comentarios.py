# -*- coding: utf-8 -*-
"""
Módulo de comentários automáticos (GPT) para os relatórios/gráficos do ATESTMED.

Disponível:
- comentar_motivos(payload, call_api=True, model='gpt-4o-mini', ...)
- comentar_overlap(md_table, chart_ascii, start, end, call_api=True, ...)
- comentar_produtividade(md_table, chart_ascii, start, end, threshold, call_api=True, ...)
- comentar_le15s(md_table_ou_org, ascii_chart=None, start=None, end=None, threshold=None, cut_n=None, call_api=True, ...)
- build_prompt_composto(payload) -> str
- comentar_composto(payload, call_api=True, ...) -> {'prompt': ..., 'comment': ...}
- chamar_gpt(system_prompt, user_prompt, call_api=True, ...) -> {'prompt','comment'}
- comentar_impacto_fila(df_all, df_sel, meta, call_api=True, ...) -> str   # NOVO

Notas:
- Todas as funções de “comentar_*” retornam **texto corrido** (um parágrafo). A exceção é
  `comentar_composto`, que retorna um dict com `prompt` e, se possível, `comment`.
"""

from __future__ import annotations
import os
import json
import re
from typing import Any, Dict, List, Optional, Tuple

# ────────────────────────────────────────────────────────────────────────────────
# .env e cliente OpenAI (compat 1.x e legado)
# ────────────────────────────────────────────────────────────────────────────────

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def _load_openai_key_from_dotenv() -> Optional[str]:
    """Carrega OPENAI_API_KEY do .env na raiz do projeto (se existir)."""
    env_path = os.path.join(BASE_DIR, ".env")
    # 1) python-dotenv
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(env_path, override=False)
    except Exception:
        # 2) parse manual se ainda não existir na env
        if os.path.exists(env_path) and not os.getenv("OPENAI_API_KEY"):
            try:
                with open(env_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue
                        k, v = line.split("=", 1)
                        if k.strip() == "OPENAI_API_KEY":
                            os.environ.setdefault("OPENAI_API_KEY", v.strip().strip('"').strip("'"))
                            break
            except Exception:
                pass
    return os.getenv("OPENAI_API_KEY")


def _call_openai(messages: List[Dict[str, str]], model: str, temperature: float) -> Optional[str]:
    """Compatível com SDK novo e legado. Retorna o texto ou None."""
    api_key = _load_openai_key_from_dotenv()
    if not api_key:
        return None

    # SDK novo (openai>=1.x)
    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
        )
        txt = (resp.choices[0].message.content or "").strip()
        if txt:
            return txt
    except Exception:
        pass

    # SDK legado
    try:
        import openai  # type: ignore
        openai.api_key = api_key
        resp = openai.ChatCompletion.create(
            model=model,
            messages=messages,
            temperature=temperature,
        )
        txt = resp["choices"][0]["message"]["content"]
        if txt:
            return txt.strip()
    except Exception:
        pass

    return None


# ────────────────────────────────────────────────────────────────────────────────
# Sistema/formatadores
# ────────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = (
    "Você é um analista de dados especializado em gestão pública e auditoria do ATESTMED. "
    "Escreva comentários claros, objetivos e tecnicamente corretos, evitando jargões, adjetivos "
    "exagerados e inferências causais. Use sempre TEXTO CORRIDO (um parágrafo)."
)

def chamar_gpt(system_prompt: str,
               user_prompt: str,
               *,
               call_api: bool = True,
               model: str = "gpt-4o-mini",
               temperature: float = 0.2) -> Dict[str, str]:
    """
    Dispara o GPT e retorna {'prompt': user_prompt, 'comment': texto_ou_vazio}.
    Se call_api=False ou sem chave, retorna apenas o prompt e comment="".
    """
    if not call_api:
        return {"prompt": user_prompt, "comment": ""}

    msg = [{"role": "system", "content": system_prompt},
           {"role": "user", "content": user_prompt}]
    txt = _call_openai(msg, model=model, temperature=temperature)
    if txt:
        return {"prompt": user_prompt, "comment": _cap_words(_to_one_paragraph(_strip_markers(txt)), 220)}
    return {"prompt": user_prompt, "comment": ""}


def _to_one_paragraph(text: str) -> str:
    text = re.sub(r"[ \t]+", " ", text or "")
    text = re.sub(r"\s*\n\s*", " ", text)
    return re.sub(r"\s{2,}", " ", text).strip()

def _strip_markers(text: str) -> str:
    """Remove cercas de código, cabeçalhos [..], tabelas md/org e diretivas org."""
    if not text:
        return ""
    text = re.sub(r"^```.*?$", "", text, flags=re.M)
    text = re.sub(r"^~~~.*?$", "", text, flags=re.M)
    kept = []
    for ln in text.splitlines():
        t = ln.strip()
        if not t:
            continue
        if t.startswith("[") and t.endswith("]"):
            continue
        if t.startswith("|"):   # tabelas
            continue
        if t.startswith("#+"):  # org directives
            continue
        kept.append(ln)
    return "\n".join(kept).strip()

def _cap_words(text: str, max_words: int) -> str:
    ws = (text or "").split()
    return " ".join(ws[:max_words]).rstrip() + ("…" if len(ws) > max_words else "")


# ────────────────────────────────────────────────────────────────────────────────
# MOTIVOS
# ────────────────────────────────────────────────────────────────────────────────

def _build_messages_motivos(payload: Dict[str, Any], max_words: int) -> List[Dict[str, str]]:
    start, end = payload.get("period", ["", ""])
    meta = payload.get("meta", {}) or {}
    lhs = meta.get("lhs_label", "Grupo")
    rhs = meta.get("rhs_label", "Brasil (excl.)")
    taxa = payload.get("nc_rate", {}) or {}
    taxa_lhs = taxa.get("lhs", None)
    taxa_rhs = taxa.get("rhs", None)
    taxa_lhs_str = f"{float(taxa_lhs):.1f}%" if isinstance(taxa_lhs, (int, float)) else "n/d"
    taxa_rhs_str = f"{float(taxa_rhs):.1f}%" if isinstance(taxa_rhs, (int, float)) else "n/d"

    rows_in = payload.get("rows", []) or []
    rows = []
    for r in rows_in:
        rows.append({
            "descricao": str(r.get("descricao", "")),
            "pct_lhs": round(float(r.get("pct_lhs", r.get("pct_perito", 0.0))), 2),
            "pct_rhs": round(float(r.get("pct_rhs", r.get("pct_brasil", 0.0))), 2),
            "n_lhs": int(r.get("n_lhs", r.get("n_perito", 0))),
            "n_rhs": int(r.get("n_rhs", r.get("n_brasil", 0))),
        })

    resumo_json = {
        "periodo": f"{start} a {end}",
        "labels": {"lhs": lhs, "rhs": rhs},
        "taxa_nc": {"lhs": taxa_lhs_str, "rhs": taxa_rhs_str},
        "rows": rows,
        "mode": payload.get("mode", "single"),
        "cuts": meta.get("cuts", {}),
        "peritos_lista": meta.get("peritos_lista", []),
    }

    user_msg = (
        "Produza um comentário interpretativo em português (Brasil), TEXTO CORRIDO (um único parágrafo, "
        f"sem títulos, sem listas, sem tabelas), com no máximo {max_words} palavras, para acompanhar o "
        f"gráfico 'Motivos de NC – {lhs} vs {rhs}' no período {start} a {end}. "
        "Inclua: (1) leitura geral das diferenças; "
        " (2) destaque de 2–4 motivos com maiores diferenças absolutas ('X% vs Y%'); "
        f" (3) contextualização com as taxas gerais de NC ({lhs}: {taxa_lhs_str}; {rhs}: {taxa_rhs_str}); "
        "evite jargões e conclusões causais. Dados (JSON):\n\n"
        + json.dumps(resumo_json, ensure_ascii=False)
    )
    return [{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg}]

def _fallback_motivos(payload: Dict[str, Any]) -> str:
    start, end = payload.get("period", ["", ""])
    meta = payload.get("meta", {}) or {}
    lhs = meta.get("lhs_label", "Grupo")
    rhs = meta.get("rhs_label", "Brasil (excl.)")
    taxa = payload.get("nc_rate", {}) or {}
    taxa_lhs = taxa.get("lhs", None)
    taxa_rhs = taxa.get("rhs", None)
    taxa_lhs_str = f"{float(taxa_lhs):.1f}%" if isinstance(taxa_lhs, (int, float)) else "n/d"
    taxa_rhs_str = f"{float(taxa_rhs):.1f}%" if isinstance(taxa_rhs, (int, float)) else "n/d"

    rows = payload.get("rows", []) or []
    diffs = sorted(
        rows,
        key=lambda r: abs(float(r.get("pct_lhs", r.get("pct_perito", 0.0))) - float(r.get("pct_rhs", r.get("pct_brasil", 0.0)))),
        reverse=True
    )[:3]

    destaques = []
    for r in diffs:
        pl = float(r.get("pct_lhs", r.get("pct_perito", 0.0)))
        pr = float(r.get("pct_rhs", r.get("pct_brasil", 0.0)))
        destaques.append(f"{r.get('descricao','')} ({pl:.2f}% vs {pr:.2f}%)")

    partes = []
    partes.append(
        f"No período {start} a {end}, observam-se diferenças na distribuição dos motivos de NC entre {lhs} e {rhs}. "
        f"As taxas gerais de NC foram {lhs}: {taxa_lhs_str} e {rhs}: {taxa_rhs_str}."
    )
    if destaques:
        partes.append("Entre os principais contrastes, destacam-se: " + "; ".join(destaques) + ".")
    partes.append("Os percentuais refletem a composição interna dos NC em cada grupo, não o volume total de tarefas.")
    return " ".join(partes)

def comentar_motivos(payload: Dict[str, Any],
                     call_api: bool = True,
                     model: str = "gpt-4o-mini",
                     temperature: float = 0.2,
                     max_words: int = 180,
                     return_prompt: bool = False) -> str:
    messages = _build_messages_motivos(payload, max_words=max_words)
    if return_prompt:
        return messages[-1]["content"]

    if call_api:
        txt = _call_openai(messages, model=model, temperature=temperature)
        if txt:
            txt = _strip_markers(txt)
            txt = _to_one_paragraph(txt)
            return _cap_words(txt, max_words)

    fb = _fallback_motivos(payload)
    fb = _to_one_paragraph(_strip_markers(fb))
    return _cap_words(fb, max_words)


# ────────────────────────────────────────────────────────────────────────────────
# OVERLAP
# ────────────────────────────────────────────────────────────────────────────────

def _parse_overlap_md_table(md: str) -> Optional[Dict[str, Any]]:
    if not md or "|" not in md:
        return None
    lines = [ln.strip() for ln in md.splitlines() if ln.strip().startswith("|") and not ln.strip().startswith("|-")]
    if len(lines) < 3:
        return None

    header_cells = [c.strip() for c in lines[0].strip("|").split("|")]
    if len(header_cells) < 4:
        return None
    a_label = header_cells[1]
    b_label = header_cells[2]

    def parse_row(s: str) -> Tuple[str, float, float, float]:
        cells = [c.strip() for c in s.strip("|").split("|")]
        lab = cells[0]
        num = cells[1].replace("s", "").strip()
        den = cells[2].replace("s", "").strip()
        pct = cells[3].replace("%", "").strip()
        return lab, float(num), float(den), float(pct)

    try:
        left_lab, left_num, left_den, left_pct = parse_row(lines[1])
        right_lab, right_num, right_den, right_pct = parse_row(lines[2])
    except Exception:
        return None

    head_low = (a_label + " " + b_label).lower()
    if "perito" in head_low:
        mode = "perito-share"
    elif "tarefa" in head_low:
        mode = "task-share"
    elif "tempo" in head_low or "(s)" in head_low or "segundo" in head_low:
        mode = "time-share"
    else:
        mode = "time-share" if any(x != int(x) for x in [left_num, left_den, right_num, right_den]) else "task-share"

    return {
        "a_label": a_label, "b_label": b_label, "mode": mode,
        "left": {"label": left_lab, "num": left_num, "den": left_den, "pct": left_pct},
        "right": {"label": right_lab, "num": right_num, "den": right_den, "pct": right_pct},
    }

def _build_messages_overlap(start: str, end: str, parsed: Dict[str, Any], ascii_chart: str, max_words: int) -> List[Dict[str, str]]:
    human_metric = {
        "perito-share": "% de peritos com sobreposição",
        "task-share":   "% de tarefas sobrepostas",
        "time-share":   "% do tempo em sobreposição"
    }.get(parsed["mode"], parsed["mode"])

    resumo = {
        "periodo": f"{start} a {end}",
        "metrica": parsed["mode"],
        "descricao_metrica": human_metric,
        "lhs": parsed["left"],
        "rhs": parsed["right"],
        "tabela_markdown": {"a_label": parsed["a_label"], "b_label": parsed["b_label"]},
        "grafico_ascii": ascii_chart or ""
    }

    user = (
        "Escreva um comentário interpretativo em português (Brasil) para acompanhar um gráfico de duas barras. "
        f"Use TEXTO CORRIDO (um único parágrafo), com no máximo {max_words} palavras. "
        "Inclua: (1) leitura direta da comparação entre os dois grupos; (2) diferença em p.p.; "
        "(3) referência aos denominadores (n) quando fizer sentido; evite conclusões causais. "
        "Dados (JSON):\n\n" + json.dumps(resumo, ensure_ascii=False)
    )
    return [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}]

def _heuristic_overlap_comment(start: str, end: str, parsed: Dict[str, Any]) -> str:
    diff = float(parsed["left"]["pct"]) - float(parsed["right"]["pct"])
    mode = parsed["mode"]
    if mode == "time-share":
        esq = f"{parsed['left']['pct']:.1f}% (n={parsed['left']['num']:.0f}/{parsed['left']['den']:.0f} s)"
        dir = f"{parsed['right']['pct']:.1f}% (n={parsed['right']['num']:.0f}/{parsed['right']['den']:.0f} s)"
    else:
        esq = f"{parsed['left']['pct']:.1f}% (n={int(parsed['left']['num'])}/{int(parsed['left']['den'])})"
        dir = f"{parsed['right']['pct']:.1f}% (n={int(parsed['right']['num'])}/{int(parsed['right']['den'])})"
    return (
        f"No período {start} a {end}, {parsed['left']['label']} apresentou {esq}, "
        f"enquanto {parsed['right']['label']} registrou {dir}. Diferença de {abs(diff):.1f} p.p., "
        f"{'acima' if diff > 0 else 'abaixo' if diff < 0 else 'em linha'} do comparativo. "
        "Percentuais refletem a composição relativa; não implicam causalidade."
    )

def comentar_overlap(md_table: str,
                     chart_ascii: str,
                     start: str,
                     end: str,
                     *,
                     call_api: bool = True,
                     model: str = "gpt-4o-mini",
                     temperature: float = 0.2,
                     max_words: int = 160,
                     return_prompt: bool = False) -> str:
    parsed = _parse_overlap_md_table(md_table or "")
    if not parsed:
        user = (
            "Escreva um comentário interpretativo em português (Brasil), TEXTO CORRIDO (um parágrafo), "
            f"máximo {max_words} palavras, para um gráfico de duas barras de sobreposição. "
            f"Período: {start} a {end}. Tabela Markdown e gráfico ASCII (se houver) a seguir:\n\n"
            f"{md_table}\n\n{chart_ascii or ''}\n"
            "Destaque a comparação direta, diferença em p.p. e os denominadores (n), sem inferir causalidade."
        )
        messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}]
        if return_prompt:
            return user
        if call_api:
            txt = _call_openai(messages, model=model, temperature=temperature)
            if txt:
                return _cap_words(_to_one_paragraph(_strip_markers(txt)), max_words)
        return _cap_words(_to_one_paragraph(_strip_markers(
            f"No período {start} a {end}, observa-se diferença entre os grupos. Abaixo, a tabela de base: {md_table}"
        )), max_words)

    messages = _build_messages_overlap(start, end, parsed, chart_ascii, max_words=max_words)
    if return_prompt:
        return messages[-1]["content"]

    if call_api:
        txt = _call_openai(messages, model=model, temperature=temperature)
        if txt:
            return _cap_words(_to_one_paragraph(_strip_markers(txt)), max_words)

    fb = _heuristic_overlap_comment(start, end, parsed)
    return _cap_words(_to_one_paragraph(_strip_markers(fb)), max_words)


# ────────────────────────────────────────────────────────────────────────────────
# PRODUTIVIDADE (≥ threshold/h)
# ────────────────────────────────────────────────────────────────────────────────

def _parse_prod_md_table(md_table: str) -> Dict[str, Any]:
    lines = [ln for ln in md_table.splitlines() if ln.strip().startswith("|")]
    if not lines:
        return {}
    hdr = re.sub(r"\s*\|\s*", " | ", lines[0].strip())
    hcols = [c.strip() for c in hdr.strip("|").split("|")]
    if len(hcols) < 4:
        return {}
    a_label = hcols[1]; b_label = hcols[2]
    data_rows: List[List[str]] = []
    for ln in lines[1:]:
        if set(ln.replace("|", "").strip()) <= set("-: "):
            continue
        cols = [c.strip() for c in re.sub(r"\s*\|\s*", " | ", ln.strip()).strip("|").split("|")]
        if len(cols) == 4:
            data_rows.append(cols)

    def _row(cols: List[str]):
        label = cols[0]
        try: num = float(cols[1].replace("s", ""))
        except: num = 0.0
        try: den = float(cols[2].replace("s", ""))
        except: den = 0.0
        try: pct = float(str(cols[3]).replace("%", "").strip())
        except: pct = 0.0
        return {"label": label, "num": num, "den": den, "pct": pct}

    left  = _row(data_rows[0]) if len(data_rows) >= 1 else {"label":"A","num":0,"den":0,"pct":0.0}
    right = _row(data_rows[1]) if len(data_rows) >= 2 else {"label":"B","num":0,"den":0,"pct":0.0}

    hint = (a_label + " " + b_label).lower()
    if "perito" in hint:
        mode = "perito-share"
    elif "tarefa" in hint:
        mode = "task-share"
    elif "tempo" in hint or "(s)" in hint or "segundo" in hint:
        mode = "time-share"
    else:
        mode = "task-share"
    return {"a_label": a_label, "b_label": b_label, "left": left, "right": right, "mode": mode}

def _build_messages_produtividade(start: str, end: str, threshold: float, mode: str,
                                  parsed: Dict[str, Any], ascii_chart: str, max_words: int) -> List[Dict[str, str]]:
    human_metric = {
        "perito-share": "% de peritos ≥ limiar",
        "task-share":   "% de tarefas de peritos ≥ limiar",
        "time-share":   "% do tempo de peritos ≥ limiar"
    }.get(mode, "métrica")

    resumo = {
        "periodo": f"{start} a {end}",
        "limiar_h": float(threshold),
        "metrica": mode,
        "descricao_metrica": human_metric,
        "lhs": parsed["left"],
        "rhs": parsed["right"],
        "labels": {"a": parsed["a_label"], "b": parsed["b_label"]},
        "grafico_ascii": ascii_chart or ""
    }

    user = (
        "Escreva um comentário interpretativo em português (Brasil) para acompanhar um gráfico de duas barras "
        f"sobre **produtividade ≥ {threshold}/h**. Use TEXTO CORRIDO (um parágrafo), "
        f"com no máximo {max_words} palavras. Inclua: (1) leitura direta da comparação; "
        "(2) diferença em p.p.; (3) referência aos denominadores (n); "
        "evite conclusões causais. Dados (JSON):\n\n" + json.dumps(resumo, ensure_ascii=False)
    )
    return [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}]

def _heuristic_prod_comment(start: str, end: str, threshold: float, mode: str, parsed: Dict[str, Any]) -> str:
    left = parsed["left"]; right = parsed["right"]
    diff = float(left["pct"]) - float(right["pct"])
    if mode == "time-share":
        esq = f"{left['pct']:.1f}% (n={left['num']:.0f}/{left['den']:.0f} s)"
        dir = f"{right['pct']:.1f}% (n={right['num']:.0f}/{right['den']:.0f} s)"
    else:
        esq = f"{left['pct']:.1f}% (n={int(left['num'])}/{int(left['den'])})"
        dir = f"{right['pct']:.1f}% (n={int(right['num'])}/{int(right['den'])})"
    return (
        f"No período {start} a {end}, considerando o limiar de {threshold}/h, "
        f"{left['label']} registrou {esq}, enquanto {right['label']} apresentou {dir}. "
        f"A diferença é de {abs(diff):.1f} p.p., "
        f"{'acima' if diff > 0 else 'abaixo' if diff < 0 else 'em linha'} do comparativo. "
        "Os percentuais refletem a participação relativa dos profissionais que atingem o limiar e podem variar conforme mix de casos e janelas de pico."
    )

def comentar_produtividade(md_table: str,
                           chart_ascii: str,
                           start: str,
                           end: str,
                           threshold: float,
                           *,
                           call_api: bool = True,
                           model: str = "gpt-4o-mini",
                           temperature: float = 0.2,
                           max_words: int = 180,
                           return_prompt: bool = False) -> str:
    parsed = _parse_prod_md_table(md_table or "")
    if not parsed:
        user = (
            "Escreva um comentário interpretativo em português (Brasil), TEXTO CORRIDO (um parágrafo), "
            f"máximo {max_words} palavras, para um gráfico de duas barras sobre produtividade (limiar {threshold}/h). "
            f"Período: {start} a {end}. Tabela Markdown e gráfico ASCII (se houver):\n\n"
            f"{md_table}\n\n{chart_ascii or ''}\n"
            "Destaque a comparação direta, diferença em p.p. e os denominadores (n), sem inferir causalidade."
        )
        messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}]
        if return_prompt:
            return user
        if call_api:
            txt = _call_openai(messages, model=model, temperature=temperature)
            if txt:
                return _cap_words(_to_one_paragraph(_strip_markers(txt)), max_words)
        basic = f"No período {start} a {end}, compara-se a proporção de profissionais ≥ {threshold}/h entre dois grupos."
        return _cap_words(_to_one_paragraph(_strip_markers(basic + " Tabela-base fornecida.")), max_words)

    mode = parsed["mode"]
    messages = _build_messages_produtividade(start, end, threshold, mode, parsed, chart_ascii, max_words=max_words)
    if return_prompt:
        return messages[-1]["content"]

    if call_api:
        txt = _call_openai(messages, model=model, temperature=temperature)
        if txt:
            return _cap_words(_to_one_paragraph(_strip_markers(txt)), max_words)

    fb = _heuristic_prod_comment(start, end, threshold, mode, parsed)
    return _cap_words(_to_one_paragraph(_strip_markers(fb)), max_words)


# ────────────────────────────────────────────────────────────────────────────────
# ≤ THRESHOLD s (ex.: ≤15s) com corte por perito (cut_n)
# ────────────────────────────────────────────────────────────────────────────────

def _parse_le15s_md_table(md_table: str) -> Dict[str, Any]:
    lines = [ln for ln in md_table.splitlines() if ln.strip().startswith("|")]
    if not lines:
        return {}
    hdr = re.sub(r"\s*\|\s*", " | ", lines[0].strip())
    hcols = [c.strip() for c in hdr.strip("|").split("|")]
    if len(hcols) < 4:
        return {}
    data_rows: List[List[str]] = []
    for ln in lines[1:]:
        if set(ln.replace("|", "").strip()) <= set("-: "):
            continue
        cols = [c.strip() for c in re.sub(r"\s*\|\s*", " | ", ln.strip()).strip("|").split("|")]
        if len(cols) == 4:
            data_rows.append(cols)

    def _row(cols: List[str]):
        label = cols[0]
        try:    leq = int(float(cols[1]))
        except: leq = 0
        try:    tot = int(float(cols[2]))
        except: tot = 0
        try:    pct = float(str(cols[3]).replace("%", "").strip())
        except: pct = 0.0
        return {"label": label, "leq": leq, "tot": tot, "pct": pct}

    left  = _row(data_rows[0]) if len(data_rows) >= 1 else {"label":"A","leq":0,"tot":0,"pct":0.0}
    right = _row(data_rows[1]) if len(data_rows) >= 2 else {"label":"B","leq":0,"tot":0,"pct":0.0}
    return {"left": left, "right": right}

def _build_messages_le15s(start: str, end: str, threshold: int, cut_n: int,
                          parsed: Dict[str, Any], ascii_chart: str, max_words: int) -> List[Dict[str, str]]:
    resumo = {
        "periodo": f"{start} a {end}",
        "threshold_s": int(threshold),
        "cut_por_perito": int(cut_n),
        "lhs": parsed["left"],
        "rhs": parsed["right"],
        "grafico_ascii": ascii_chart or ""
    }
    user = (
        "Escreva um comentário interpretativo em português (Brasil) para um gráfico de duas barras sobre o "
        f"% de perícias ≤ {threshold}s, com corte por perito (apenas peritos com ≥{cut_n} tarefas ≤ {threshold}s entram no numerador). "
        "Use TEXTO CORRIDO (um parágrafo), com no máximo "
        f"{max_words} palavras. Inclua: (1) leitura direta; (2) diferença em p.p.; "
        "(3) numeradores/denominadores; (4) ressalva amostral se aplicável; evite causalidade. "
        "Dados (JSON):\n\n" + json.dumps(resumo, ensure_ascii=False)
    )
    return [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}]

def _heuristic_le15s_comment(start: str, end: str, threshold: int, cut_n: int, parsed: Dict[str, Any]) -> str:
    L = parsed["left"]; R = parsed["right"]
    dpp = float(L["pct"]) - float(R["pct"])
    alert = []
    if int(L["tot"]) < 50: alert.append(f"{L['label'].lower()} com amostra reduzida")
    if int(R["tot"]) < 50: alert.append(f"{R['label'].lower()} com amostra reduzida")
    alerta = f" Atenção: {', '.join(alert)}." if alert else ""
    return (
        f"No período {start} a {end}, com limiar ≤{threshold}s e corte por perito (≥{cut_n} tarefas ≤{threshold}s), "
        f"{L['label']} registrou {L['pct']:.1f}% ({int(L['leq'])}/{int(L['tot'])}) e {R['label']} apresentou "
        f"{R['pct']:.1f}% ({int(R['leq'])}/{int(R['tot'])}), diferença de {abs(dpp):.1f} p.p. "
        "Os percentuais refletem a participação relativa de tarefas muito curtas entre os elegíveis e podem variar com "
        "o mix de casos e horários de pico; recomenda-se inspecionar a distribuição de durações e a consistência de registros."
        + alerta
    )

def _extract_le15s_from_org(org_txt: str) -> Tuple[str, str, int, int, Dict[str, Any]]:
    """Permite usar comentar_le15s(org_txt, call_api=...). Extrai start,end,threshold,cut_n e tabela MD do .org."""
    start = end = ""
    threshold = 15
    cut_n = 10
    table_lines: List[str] = []
    for ln in org_txt.splitlines():
        s = ln.strip()
        if s.startswith(":PERIODO:"):
            try:
                per = s.split(":", 2)[-1].strip()
                start, _, end = per.partition(" a ")
                start = start.strip(); end = end.strip()
            except Exception:
                pass
        elif s.startswith(":THRESHOLD:"):
            try:
                threshold = int(re.findall(r"(\d+)", s)[0])
            except Exception:
                pass
        elif s.startswith(":CUT_N:"):
            try:
                cut_n = int(re.findall(r"(\d+)", s)[0])
            except Exception:
                pass
        elif s.startswith("|"):
            table_lines.append(s)
    md_table = "\n".join(table_lines)
    parsed = _parse_le15s_md_table(md_table)
    return start, end, threshold, cut_n, parsed

def comentar_le15s(md_table_or_org: str,
                   ascii_chart: Optional[str] = None,
                   start: Optional[str] = None,
                   end: Optional[str] = None,
                   threshold: Optional[int] = None,
                   cut_n: Optional[int] = None,
                   *,
                   call_api: bool = True,
                   model: str = "gpt-4o-mini",
                   temperature: float = 0.2,
                   max_words: int = 180,
                   return_prompt: bool = False) -> str:
    """
    Flexível:
      1) Forma estruturada → comentar_le15s(md_table, ascii_chart, start, end, threshold, cut_n, ...)
      2) Forma compat .org  → comentar_le15s(org_text, call_api=True, ...)   # auto-extrai dados
    """
    # Detecta se é um .org completo (tem :PROPERTIES: ou '#+CAPTION' etc.)
    is_org_blob = ((":PROPERTIES:" in (md_table_or_org or "")) or ("#+CAPTION" in (md_table_or_org or "")))
    if is_org_blob and (not start or not end or threshold is None or cut_n is None):
        extracted_start, extracted_end, th, cn, parsed = _extract_le15s_from_org(md_table_or_org)
        start = start or extracted_start
        end = end or extracted_end
        threshold = threshold if threshold is not None else th
        cut_n = cut_n if cut_n is not None else cn
        md_table = md_table_or_org  # já contém a tabela
    else:
        parsed = _parse_le15s_md_table(md_table_or_org or "")
        md_table = md_table_or_org

    # Segurança de tipos
    start = start or ""
    end = end or ""
    threshold = int(threshold or 15)
    cut_n = int(cut_n or 10)

    if not parsed:
        user = (
            "Escreva um comentário interpretativo em português (Brasil), TEXTO CORRIDO (um parágrafo), "
            f"máximo {max_words} palavras, para um gráfico de duas barras sobre o % de perícias ≤ {threshold}s, "
            f"com corte por perito (≥{cut_n}). Período: {start} a {end}. Tabela/Org e gráfico ASCII (se houver):\n\n"
            f"{md_table}\n\n{ascii_chart or ''}\n"
            "Destaque a comparação direta, diferença em p.p. e os denominadores (n), sem inferir causalidade."
        )
        messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}]
        if return_prompt:
            return user
        if call_api:
            txt = _call_openai(messages, model=model, temperature=temperature)
            if txt:
                return _cap_words(_to_one_paragraph(_strip_markers(txt)), max_words)
        basic = f"No período {start} a {end}, compara-se o % de perícias ≤{threshold}s com corte por perito (≥{cut_n})."
        return _cap_words(_to_one_paragraph(_strip_markers(basic + " Tabela-base fornecida.")), max_words)

    messages = _build_messages_le15s(start, end, int(threshold), int(cut_n), parsed, ascii_chart or "", max_words=max_words)
    if return_prompt:
        return messages[-1]["content"]

    if call_api:
        txt = _call_openai(messages, model=model, temperature=temperature)
        if txt:
            return _cap_words(_to_one_paragraph(_strip_markers(txt)), max_words)

    fb = _heuristic_le15s_comment(start, end, int(threshold), int(cut_n), parsed)
    return _cap_words(_to_one_paragraph(_strip_markers(fb)), max_words)


# ────────────────────────────────────────────────────────────────────────────────
# COMPOSTO (painel de 4 indicadores + linhas de referência)
# ────────────────────────────────────────────────────────────────────────────────

def build_prompt_composto(payload: Dict[str, Any], max_words: int = 180) -> str:
    """
    Retorna apenas o prompt do usuário para o modelo (para auditoria/registro).
    """
    start, end = payload.get("period", ["", ""])
    grp_title  = payload.get("grp_title", "Grupo")
    alvo_prod  = payload.get("alvo_prod", 50)
    G = payload.get("metrics", {})        # grupo
    B = payload.get("br_metrics", {})     # BR-excl.
    S = payload.get("br_stats", {})       # linhas
    cuts = payload.get("cuts", {}) or {}
    cut_hits = payload.get("cut_hits", {})

    resumo = {
        "periodo": [start, end],
        "grupo": grp_title,
        "alvo_prod_h": alvo_prod,
        "barras": {"grupo": G, "br_excl": B},
        "linhas_br_excl": S,
        "cuts": cuts,
        "cut_hits": cut_hits,
    }

    user = (
        "Escreva um comentário (português Brasil) para acompanhar um gráfico com 4 barras "
        f"(Indicadores compostos: % NC, Prod (% do alvo), ≤15s (%), Sobreposição (%)) para {grp_title} vs BR-excl., "
        "e linhas de referência (média, mediana, média+DP do BR-excl.). "
        "Use TEXTO CORRIDO em 3–4 frases (um parágrafo), máx. {max_words} palavras. "
        "Inclua: leitura comparativa por indicador; menção a quais indicadores mais pesam; eventuais limitações "
        "(amostras pequenas, dispersão, outliers); e uma ação objetiva de verificação. "
        "Dados (JSON):\n\n" + json.dumps(resumo, ensure_ascii=False)
    ).replace("{max_words}", str(max_words))
    return user

def comentar_composto(payload: Dict[str, Any],
                      *,
                      call_api: bool = True,
                      model: str = "gpt-4o-mini",
                      temperature: float = 0.2,
                      max_words: int = 180) -> Dict[str, str]:
    """
    Retorna {'prompt': ..., 'comment': ...}. O comentário é um parágrafo.
    """
    user_prompt = build_prompt_composto(payload, max_words=max_words)
    out = chamar_gpt(SYSTEM_PROMPT, user_prompt, call_api=call_api, model=model, temperature=temperature)
    # Limita tamanho do comment
    if out.get("comment"):
        out["comment"] = _cap_words(out["comment"], max_words)
    return out


# ────────────────────────────────────────────────────────────────────────────────
# IMPACTO NA FILA — comentário geral para o relatório do impacto_fila.py  (NOVO)
# ────────────────────────────────────────────────────────────────────────────────

def comentar_artefato(kind: str, ctx: dict, **kwargs) -> str:
    # pode evoluir depois; por enquanto, retorna vazio para não quebrar
    return ""

def _system_prompt_impacto() -> str:
    """
    System prompt específico do Impacto na Fila:
    - força a leitura de 'selecionados' como alto risco e impacto negativo
    - explicita que Top N = piores peritos do período (maior IV)
    """
    return (
        "Você é um analista do ATESTMED, conciso e técnico. "
        "IMPORTANTÍSSIMO: trate métricas elevadas de produtividade ≥ limiar, tarefas ≤15s e sobreposição como "
        "indicadores de risco; interprete 'selecionados' como perfis de maior probabilidade de não conformidade, "
        "logo com **impacto negativo** na fila. "
        "Quando houver ranking *Top N* (ex.: Top 10), interprete-o como **os piores peritos do período** "
        "(maior IV) e diga isso explicitamente, relacionando ao **maior impacto na fila**. "
        "Evite jargões e **não** faça inferências causais."
    )

def _build_prompt_impacto_fila(df_all: Any, df_sel: Any, meta: Dict[str, Any], max_words: int) -> Tuple[str, Dict[str, Any]]:
    """
    Monta o prompt (usuário) e um payload JSON enxuto com os campos relevantes.
    Aceita DataFrames do pandas, mas evita dependência explícita.
    """
    # Extrai Top N ordenado por IV_vagas (se existir)
    topn = int(meta.get("topn", 10) or 10)

    def _iter_rows(df):
        # tenta DataFrame -> iterrows; se falhar, tenta lista de dicts
        if hasattr(df, "iterrows"):
            return df.iterrows()
        elif isinstance(df, list):
            return enumerate(df)
        else:
            return enumerate([])

    # Tentamos ordenar por IV_vagas desc
    try:
        if hasattr(df_sel, "sort_values"):
            df_top = df_sel.sort_values("IV_vagas", ascending=False).head(topn)
        else:
            df_top = df_sel  # melhor esforço
    except Exception:
        df_top = df_sel

    top_items: List[Dict[str, Any]] = []
    try:
        for _, r in _iter_rows(df_top):
            # r pode ser Series/obj dict-like
            get = (lambda k, default=None: (r[k] if (hasattr(r, "__getitem__") and k in r) else getattr(r, k, default)))
            top_items.append({
                "perito":      str(get("nomePerito", "")),
                "IV_vagas":    int(float(get("IV_vagas", 0) or 0)),
                "N":           int(float(get("N", 0) or 0)),
                "NC":          int(float(get("NC", 0) or 0)),
                "Excesso_E":   int(float(get("E", 0) or 0)),
                "score_final": None if get("score_final", None) in (None, "", "nan") else float(get("score_final")),
                "cr":          str(get("cr", "—") or "—"),
                "dr":          str(get("dr", "—") or "—"),
                "uo":          str(get("uo", "—") or "—"),
            })
            if len(top_items) >= topn:
                break
    except Exception:
        top_items = []

    payload = {
        "periodo": [meta.get("start"), meta.get("end")],
        "alpha": float(meta.get("alpha", 0.0) or 0.0),
        "p_br": float(meta.get("p_br", 0.0) or 0.0),
        "iv_total_periodo": int(meta.get("iv_total_period", 0) or 0),
        "iv_total_selecionado": int(meta.get("iv_total_sel", 0) or 0),
        "peso_selecionados_pct": round(float(meta.get("peso_sel", 0.0) or 0.0) * 100, 2),
        "delta_tmea_periodo_dias": meta.get("delta_tmea_period"),
        "delta_tmea_sel_dias": meta.get("delta_tmea_sel"),
        "topn": topn,
        "score_cut": meta.get("score_cut"),
        "n_all_peritos": int(meta.get("n_all", 0) or 0),
        "n_sel_peritos": int(meta.get("n_sel", 0) or 0),
        "top": top_items,
    }

    user_prompt = (
        "Escreva um **único parágrafo** (pt-BR), ≤ {max_words} palavras, claro e objetivo, "
        "para o *Resumo do Impacto na Fila*. Diga o que foi estimado no período, destaque que os **selecionados** "
        "representam **perfis de alto risco** e concentram **impacto negativo**. "
        "Deixe explícito que o **Top N** corresponde aos **piores peritos do período (maior IV)** e ao **maior impacto** na fila, "
        "quantifique o **peso** dos selecionados (% do IV do período) e, ao final, recomende **1 ação objetiva** "
        "(ex.: auditoria direcionada, supervisão clínica, reforço de padrões, monitoramento). "
        "Evite causalidade. Dados (JSON):\n\n"
        + json.dumps(payload, ensure_ascii=False)
    ).replace("{max_words}", str(max_words))

    return user_prompt, payload

def _fallback_impacto_fila(meta: Dict[str, Any]) -> str:
    """Fallback curto e assertivo quando a API não estiver disponível."""
    start = meta.get("start", "—"); end = meta.get("end", "—")
    topn = int(meta.get("topn", 10) or 10)
    ivp = int(meta.get("iv_total_period", meta.get("iv_total_periodo", 0)) or 0)
    ivs = int(meta.get("iv_total_sel", meta.get("iv_total_selecionado", 0)) or 0)
    peso = float(meta.get("peso_sel", 0.0) or 0.0) * 100.0
    delta_p = meta.get("delta_tmea_period", meta.get("delta_tmea_periodo_dias", None))
    delta_s = meta.get("delta_tmea_sel", meta.get("delta_tmea_sel_dias", None))
    delta_p_str = f"{int(delta_p)} d" if isinstance(delta_p, (int, float)) else "n/d"
    delta_s_str = f"{int(delta_s)} d" if isinstance(delta_s, (int, float)) else "n/d"

    txt = (
        f"No período {start} a {end}, estimamos **{ivp} vagas** adicionadas à fila (impacto negativo). "
        f"O grupo **selecionado** concentrou **{ivs} vagas** (~{peso:.1f}% do total), indicando **perfis de alto risco**. "
        f"O **Top {topn}** corresponde aos **piores peritos do período** (maior IV) e responde pela maior parcela do impacto. "
        f"A projeção sugere ΔTMEA período≈ {delta_p_str} (dos quais {delta_s_str} atribuíveis aos selecionados). "
        "Recomenda-se **auditoria direcionada e supervisão** sobre esses perfis, com monitoramento contínuo dos indicadores."
    )
    return _to_one_paragraph(_strip_markers(txt))

def comentar_impacto_fila(df_all: Any,
                          df_sel: Any,
                          meta: Dict[str, Any],
                          *,
                          call_api: bool = True,
                          model: str = "gpt-4o-mini",
                          temperature: float = 0.2,
                          max_words: int = 200) -> str:
    """
    Comentário geral do relatório de Impacto na Fila (um parágrafo).
    Assinatura compatível com impacto_fila.py:
      comentar_impacto_fila(df_all.copy(), df_sel.copy(), dict(meta))
    """
    try:
        user_prompt, _payload = _build_prompt_impacto_fila(df_all, df_sel, meta, max_words=max_words)
    except Exception:
        # Se der algo errado ao montar payload, usa fallback direto
        return _cap_words(_fallback_impacto_fila(meta), max_words)

    if not call_api:
        return _cap_words(_fallback_impacto_fila(meta), max_words)

    out = chamar_gpt(_system_prompt_impacto(), user_prompt, call_api=True, model=model, temperature=temperature)
    comment = (out.get("comment") or "").strip()
    if comment:
        return _cap_words(comment, max_words)

    # Fallback final
    return _cap_words(_fallback_impacto_fila(meta), max_words)

# ────────────────────────────────────────────────────────────────────────────────
# FLUXO B — prompts por gráfico/tabela + despachante genérico
# ────────────────────────────────────────────────────────────────────────────────

def _ctx_get(ctx: dict, key: str, default=None):
    try:
        v = ctx.get(key, default)
        # floats podem vir como str:
        if key in {"soma_impacto","media_nc_brasil","gini","mediana_global","p90_global"} and isinstance(v, str):
            try: return float(v)
            except: return default
        return v
    except Exception:
        return default

PROMPTS_FB = {
    # FIGURAS
    "impacto_top20": (
        "Comente o gráfico 'Impacto na fila — Top 20 elegíveis'. "
        "Período: {periodo_inicio} a {periodo_fim}. Modo de impacto: {impact_mode}. "
        "Elegíveis considerados no relatório: {n_elegiveis}. Soma do impacto reportado: {soma_impacto:.0f}. "
        "Explique o que o ranking mostra, como interpretar as barras e que leitura gerencial fazer "
        "(concentração, cauda longa, priorização). Não invente números."
    ),
    "heatmap_score_flags": (
        "Comente o heatmap 'Critérios acionados (até 10 peritos)'. "
        "Período: {periodo_inicio} a {periodo_fim}. Média nacional de NC: {media_nc_brasil:.2f}%. "
        "Explique cada coluna (Prod≥50/h, Overlap, ≤15s≥10, %NC≥2×BR), como interpretar combinações de flags "
        "e o que priorizar na prática."
    ),
    "dist_por_CR": (
        "Comente a distribuição por CR (contagem baseada em {contagem_base}). "
        "Período: {periodo_inicio} a {periodo_fim}. Indique concentração, dispersão e como usar a visão "
        "para priorização regional."
    ),
    "dist_por_DR": (
        "Comente a distribuição por DR (contagem baseada em {contagem_base}). "
        "Aponte onde há maior representatividade e como isso orienta focos operacionais."
    ),
    "dist_por_UF": (
        "Comente a distribuição por UF (contagem baseada em {contagem_base}). "
        "Discuta diferenças esperadas/inesperadas e como isso apoia a governança local."
    ),
    "indicadores_medios": (
        "Comente 'Indicadores médios — Elegíveis' (Prod/h média vs alvo 50/h, %NC, ≤15s%, Overlap%). "
        "Período: {periodo_inicio} a {periodo_fim}. Aponte lacunas frente ao alvo e riscos operacionais."
    ),
    "impacto_esperado_pos_medidas_top20": (
        "Comente 'Impacto esperado pós-medidas — Top 20'. "
        "Explique a lógica das medidas (produtividade, NC, ≤15s, sobreposição), hipóteses e limitações, "
        "e como priorizar ações de curto prazo."
    ),
    "lorenz_impacto_elegiveis": (
        "Comente a 'Curva de Lorenz — Impacto entre elegíveis' com índice de Gini={gini:.3f}. "
        "Explique o que a curvatura e o Gini indicam sobre concentração do impacto e prioridades."
    ),
    "pareto_impacto": (
        "Comente o 'Pareto por impacto (Top-20)'. "
        "Explique a leitura de barras vs. cumulativo e como usar o ponto de corte para foco."
    ),
    "duracao_mediana_p90_top20": (
        "Comente 'Duração por perito — Mediana e P90 (Top-20 por volume)'. "
        "Resumo global: mediana {mediana_global:.1f}s; P90 {p90_global:.1f}s. "
        "Explique o que isso diz sobre fila/variabilidade e implicações operacionais."
    ),
    "cenarios_mensais": (
        "Comente 'Impacto na fila — Real vs Cenários' ({rotulos_cenarios}) aplicados sobre IV_sel (Top-{topk_mensal} mensal). "
        "Explique o que significa reduzir {reducoes_descritas} do IV_sel e como a simulação orienta metas."
    ),
    # TABELAS / SEÇÕES
    "tabela_principal_csv": (
        "Comente a 'Tabela principal (ranking)' dos elegíveis ao Fluxo B. "
        "Explique colunas-chave (Prod/h, %NC, ≤15s, Overlap, Score v2, Impacto) e como ler o ranking para priorização."
    ),
    "tabela_full_csv": (
        "Comente a 'Tabela completa de peritos' (apêndice). "
        "Oriente como ela apoia auditoria, drill-down e checagens de consistência."
    ),
    "plano_acao_csv": (
        "Comente o 'Plano de ação (CSV)'. "
        "Explique colunas de ganho esperado e gargalos e como usar na rotina de acompanhamento."
    ),
    "cenarios_csv": (
        "Comente a tabela 'Cenários mensais (CSV)' (IV_total, IV_sel e reduções por rótulo). "
        "Explique como interpretar os deltas mensais e o total."
    ),
    "files_section": (
        "Escreva 2–3 linhas de guia rápido para a seção 'Arquivos gerados', indicando por onde começar e "
        "como navegar entre PDF, ORG, CSVs e figuras."
    ),
}

def _build_prompt_fb(kind: str, ctx: dict, max_words: int) -> str:
    tpl = PROMPTS_FB.get(kind)
    if not tpl:
        return f"Escreva um comentário breve e objetivo (≤{max_words} palavras) para o artefato '{kind}'. Contexto: {json.dumps(ctx, ensure_ascii=False)}"
    # valores com defaults seguros
    safe_ctx = {
        "periodo_inicio":   _ctx_get(ctx, "periodo_inicio", ""),
        "periodo_fim":      _ctx_get(ctx, "periodo_fim", ""),
        "impact_mode":      _ctx_get(ctx, "impact_mode", "combined"),
        "n_elegiveis":      int(_ctx_get(ctx, "n_elegiveis", 0) or 0),
        "soma_impacto":     float(_ctx_get(ctx, "soma_impacto", 0.0) or 0.0),
        "media_nc_brasil":  float(_ctx_get(ctx, "media_nc_brasil", 0.0) or 0.0),
        "contagem_base":    _ctx_get(ctx, "contagem_base", "peritos"),
        "gini":             float(_ctx_get(ctx, "gini", 0.0) or 0.0),
        "mediana_global":   float(_ctx_get(ctx, "mediana_global", 0.0) or 0.0),
        "p90_global":       float(_ctx_get(ctx, "p90_global", 0.0) or 0.0),
        "topk_mensal":      int(_ctx_get(ctx, "topk_mensal", 10) or 10),
        "rotulos_cenarios": _ctx_get(ctx, "rotulos_cenarios", "A, B, C"),
        "reducoes_descritas": _ctx_get(ctx, "reducoes_descritas", "50%, 70%, 100%"),
    }
    user = (
        f"Produza um comentário interpretativo em português (Brasil), TEXTO CORRIDO (um parágrafo), "
        f"com no máximo {max_words} palavras, para o artefato '{kind}'. "
        + tpl.format(**safe_ctx)
    )
    return user

def comentar_artefato(kind: str,
                      ctx: dict,
                      *,
                      call_api: bool = True,
                      model: str = "gpt-4o-mini",
                      temperature: float = 0.2,
                      max_words: int = 180,
                      return_prompt: bool = False) -> str:
    """
    Comentário genérico para gráficos/tabelas do Fluxo B.
    - kind: uma das chaves de PROMPTS_FB
    - ctx: dicionário com os placeholders esperados
    """
    user_prompt = _build_prompt_fb(kind, ctx, max_words=max_words)
    if return_prompt:
        return user_prompt
    out = chamar_gpt(SYSTEM_PROMPT, user_prompt, call_api=call_api, model=model, temperature=temperature)
    txt = (out.get("comment") or "").strip()
    if not txt:
        # fallback simples: devolve o próprio prompt resumido
        return _cap_words(_to_one_paragraph(_strip_markers(user_prompt)), max_words)
    return _cap_words(_to_one_paragraph(_strip_markers(txt)), max_words)

