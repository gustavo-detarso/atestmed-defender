#!/usr/bin/env python3
import os
import sys
import datetime
import subprocess
import re
import sqlite3
from asciimatics.screen import Screen
from pathlib import Path
from utils.db_manager import create_database, load_csv_to_db, calcular_indicadores

BANNER = [
    "╔══════════════════════════════════════════════════════════════╗",
    "║              ATESTMED MONITORAMENTO CLI - BBS              ║",
    "╚══════════════════════════════════════════════════════════════╝"
]
HELP_MAIN = "↑↓ Navegar  ENTER Escolher  Q Sair"
HELP_SUB  = "↑↓ Navegar  ENTER Escolher  < Voltar  Q Sair"
HELP_DATE = "Digite data (YYYY-MM-DD)  ENTER confirmar  < Voltar"
HELP_MSG  = "ENTER para continuar"
HELP_ARG  = "Preencha o argumento obrigatório (ENTER confirma, < volta)"
HELP_AUTOCOMP = "Digite para buscar, ↑↓ para navegar, ENTER confirma, < volta"
BOX_COLOR = Screen.COLOUR_CYAN
SELECT_COLOR = Screen.COLOUR_YELLOW
BG_SELECT = Screen.COLOUR_BLUE
NORMAL_COLOR = Screen.COLOUR_WHITE
BORDER_COLOR = Screen.COLOUR_MAGENTA

GRAPHS_DIR = "graphs_and_tables"
EXPORT_DIR = "exports"
DEBUG_LOG_DIR = "debug_logs"
DEBUG_LOG_PATH = os.path.join(DEBUG_LOG_DIR, "cli_tui_debug.log")
Path(DEBUG_LOG_DIR).mkdir(exist_ok=True)
DB_PATH = os.path.join("db", "atestmed.db")

def handle_db(screen):
    # 1️⃣ Desenha banner BBS
    screen.clear()
    for y, line in enumerate(BANNER):
        screen.print_at(line,
                        (screen.width - len(line)) // 2,
                        y + 1,
                        colour=BOX_COLOR,
                        attr=Screen.A_BOLD)
    screen.refresh()

    # 2️⃣ Caminho do DB
    db_dir = "db"
    db_path = os.path.join(db_dir, "atestmed.db")

    # 3️⃣ Se não existir: perguntar para criar
    if not os.path.exists(db_path):
        if not tela_yesno(screen, "Banco não encontrado. Criar agora?"):
            return
        create_database()
        calcular_indicadores(db_path)
        tela_mensagem(screen, "✅ Banco criado e indicadores calculados!", cor=Screen.COLOUR_GREEN)
        return

    # 4️⃣ Se existir: submenu Recriar / Atualizar
    escolha = tela_submenu(screen, "Gerenciar Banco de Dados", ["Recriar", "Atualizar"])
    if escolha is None:
        return
    if escolha == 0:  # Recriar
        if tela_yesno(screen, "Tem certeza que quer excluir e recriar?"):
            os.remove(db_path)
            create_database()
            calcular_indicadores(db_path)
            tela_mensagem(screen, "✅ Banco recriado com sucesso!", cor=Screen.COLOUR_GREEN)
        else:
            return
    # escolha == 1 → Atualizar apenas

    # 5️⃣ Importação de CSVs
    files = [f for f in os.listdir("data/raw") if f.lower().endswith(".csv")]
    modo = tela_selecao_csv(screen, files)
    if modo == "Cancelar":
        return

    to_load = files if modo == "Todos" else [
        f for f in files if tela_yesno(screen, f"Carregar '{f}'?")
    ]

    for f in to_load:
        load_csv_to_db(os.path.join("data/raw", f), db_path)
    calcular_indicadores(db_path)
    tela_mensagem(screen, "📥 CSVs importados e indicadores atualizados!", cor=Screen.COLOUR_GREEN)

def log_debug(msg):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {msg}\n")

def bbs_box(screen, lines, help_line=None, selected=None):
    box_w = max(len(line) for line in lines) + 6
    box_h = len(lines) + 4
    x = (screen.width - box_w) // 2
    y = (screen.height - box_h) // 2
    screen.print_at("╔" + "═"*(box_w-2) + "╗", x, y, colour=BORDER_COLOR, attr=Screen.A_BOLD)
    for i, line in enumerate(lines):
        if selected is not None and i == selected:
            content = f"→ {line.ljust(box_w-6)} "
            screen.print_at("║", x, y+i+1, colour=BORDER_COLOR, attr=Screen.A_BOLD)
            screen.print_at(content, x+1, y+i+1, colour=SELECT_COLOR, bg=BG_SELECT, attr=Screen.A_BOLD)
            screen.print_at("║", x+box_w-1, y+i+1, colour=BORDER_COLOR, attr=Screen.A_BOLD)
        else:
            content = f"  {line.ljust(box_w-6)} "
            screen.print_at("║", x, y+i+1, colour=BORDER_COLOR, attr=Screen.A_BOLD)
            screen.print_at(content, x+1, y+i+1, colour=NORMAL_COLOR, attr=Screen.A_BOLD)
            screen.print_at("║", x+box_w-1, y+i+1, colour=BORDER_COLOR, attr=Screen.A_BOLD)
    screen.print_at("╚" + "═"*(box_w-2) + "╝", x, y+box_h-1, colour=BORDER_COLOR, attr=Screen.A_BOLD)
    if help_line:
        help_text = f"│ {help_line} │"
        screen.print_at(help_text, (screen.width - len(help_text)) // 2, y + box_h, colour=Screen.COLOUR_GREEN, attr=Screen.A_BOLD)
    screen.refresh()

def listar_scripts_estatisticas(pasta):
    try:
        scripts = []
        for nome in sorted(os.listdir(pasta)):
            if nome.endswith(".py") and not nome.startswith("_"):
                scripts.append(nome[:-3])
        return scripts
    except Exception as e:
        log_debug(f"Erro listando scripts: {e}")
        return []

def listar_scripts_r():
    pasta = os.path.join("r_stats", "scripts_r")
    try:
        return sorted([
            nome for nome in os.listdir(pasta)
            if nome.endswith(".Rmd") and not nome.startswith("_")
        ])
    except Exception as e:
        log_debug(f"Erro listando scripts Rmd: {e}")
        return []

def tela_menu(screen):
    opcoes = ["Gerenciar Banco de Dados", "Gráficos e Tabelas", "Análises em R", "Sair"]
    idx = 0
    needs_redraw = True
    while True:
        if needs_redraw:
            screen.clear()
            for y, line in enumerate(BANNER):
                screen.print_at(line, (screen.width - len(line)) // 2, y + 1, colour=BOX_COLOR, attr=Screen.A_BOLD)
            bbs_box(screen, [f"{i+1}. {op}" for i, op in enumerate(opcoes)], HELP_MAIN, selected=idx)
            needs_redraw = False
        ev = screen.get_key()
        if ev in (ord("q"), ord("Q")):
            log_debug("Usuário saiu no menu principal")
            return None
        if ev == Screen.KEY_DOWN:
            idx = (idx + 1) % len(opcoes)
            needs_redraw = True
        elif ev == Screen.KEY_UP:
            idx = (idx - 1) % len(opcoes)
            needs_redraw = True
        elif ev in (10, 13):
            log_debug(f"Usuário selecionou menu: {opcoes[idx]}")
            return idx

def tela_submenu_estatisticas(screen, scripts):
    idx = 0
    needs_redraw = True
    opcoes = scripts + ["Voltar"]
    while True:
        if needs_redraw:
            screen.clear()
            for y, line in enumerate(BANNER):
                screen.print_at(line, (screen.width - len(line)) // 2, y + 1, colour=BOX_COLOR, attr=Screen.A_BOLD)
            bbs_box(screen, [f"{i+1}. {nome}" for i, nome in enumerate(opcoes)], HELP_SUB, selected=idx)
            screen.print_at("Estatísticas".center(screen.width), 0, len(BANNER) + 1, colour=Screen.COLOUR_GREEN, attr=Screen.A_BOLD)
            needs_redraw = False
        ev = screen.get_key()
        if ev == ord('<'):
            log_debug("Usuário pressionou < no submenu Estatísticas")
            return None
        if ev in (ord("q"), ord("Q")):
            log_debug("Usuário saiu no submenu Estatísticas")
            sys.exit(0)
        if ev == Screen.KEY_DOWN:
            idx = (idx + 1) % len(opcoes)
            needs_redraw = True
        elif ev == Screen.KEY_UP:
            idx = (idx - 1) % len(opcoes)
            needs_redraw = True
        elif ev in (10, 13):
            log_debug(f"Usuário selecionou estatística: {opcoes[idx]}")
            if idx == len(opcoes)-1:
                return None  # Voltar
            return idx

def tela_data(screen, prompt):
    date = ""
    needs_redraw = True
    while True:
        clean = date.replace("-", "")[:8]
        formatted = ""
        for i, c in enumerate(clean):
            if i == 4 or i == 6:
                formatted += "-"
            formatted += c
        show_date = formatted

        if needs_redraw:
            screen.clear()
            for y, line in enumerate(BANNER):
                screen.print_at(line, (screen.width - len(line)) // 2, y + 1, colour=BOX_COLOR, attr=Screen.A_BOLD)
            lines = [prompt, f"> {show_date}"]
            bbs_box(screen, lines, HELP_DATE)
            needs_redraw = False
        ev = screen.get_key()
        if ev == ord('<'):  # "<" para voltar (SHIFT + ,)
            log_debug("Usuário pressionou < em input de data")
            return None
        if ev in (ord("q"), ord("Q")):
            log_debug("Usuário saiu em input de data")
            sys.exit(0)
        if ev == Screen.KEY_BACK and date:
            date = date[:-1]
            needs_redraw = True
            continue
        if ev in (10, 13):
            try:
                if len(clean) == 8:
                    val = f"{clean[:4]}-{clean[4:6]}-{clean[6:8]}"
                    datetime.datetime.strptime(val, "%Y-%m-%d")
                    log_debug(f"Usuário digitou data: {val}")
                    return val
            except ValueError:
                continue
        if isinstance(ev, int) and 48 <= ev <= 57 and len(clean) < 8:
            date += chr(ev)
            needs_redraw = True
            continue

def tela_input(screen, prompt):
    val = ""
    needs_redraw = True
    while True:
        if needs_redraw:
            screen.clear()
            for y, line in enumerate(BANNER):
                screen.print_at(line, (screen.width - len(line)) // 2, y + 1, colour=BOX_COLOR, attr=Screen.A_BOLD)
            lines = [prompt, f"> {val}"]
            bbs_box(screen, lines, HELP_ARG)
            needs_redraw = False
        ev = screen.get_key()
        if ev == ord('<'):
            log_debug("Usuário pressionou < em input de argumento")
            return None
        if ev in (ord("q"), ord("Q")):
            log_debug("Usuário saiu em input de argumento")
            sys.exit(0)
        if ev == Screen.KEY_BACK and val:
            val = val[:-1]
            needs_redraw = True
            continue
        if ev in (10, 13):
            log_debug(f"Usuário digitou argumento: {prompt} => {val.strip()}")
            return val.strip()
        if isinstance(ev, int) and 32 <= ev < 127:
            ch = chr(ev)
            val += ch
            needs_redraw = True
            continue

def tela_autocomplete(screen, prompt, options):
    filtro = ""
    idx = 0
    scroll_offset = 0
    PAGE_SIZE = min(20, screen.height - 10)
    last_render = None

    while True:
        filtered = [opt for opt in options if filtro.lower() in opt.lower()]
        total = len(filtered)
        idx = max(0, min(idx, total - 1))
        if idx < scroll_offset:
            scroll_offset = idx
        elif idx >= scroll_offset + PAGE_SIZE:
            scroll_offset = idx - PAGE_SIZE + 1

        show = filtered[scroll_offset:scroll_offset+PAGE_SIZE]
        lines = [prompt, f"Filtro: {filtro}"]
        for i, opt in enumerate(show):
            actual_idx = i + scroll_offset
            line = f"> {opt}" if actual_idx == idx else f"  {opt}"
            lines.append(line)
        if not filtered:
            lines.append("[nenhum encontrado]")

        render_state = (tuple(lines), idx+2-scroll_offset)
        if render_state != last_render:
            screen.clear()
            bbs_box(screen, lines, HELP_AUTOCOMP, selected=idx+2-scroll_offset)
            screen.refresh()
            last_render = render_state

        ev = screen.get_key()
        redraw = False
        if ev == ord('<'):
            log_debug("Usuário pressionou < em autocomplete")
            return None
        if ev in (ord("q"), ord("Q")):
            log_debug("Usuário saiu em autocomplete")
            sys.exit(0)
        if ev == Screen.KEY_DOWN and filtered:
            idx = (idx + 1) % max(1, len(filtered))
            redraw = True
        elif ev == Screen.KEY_UP and filtered:
            idx = (idx - 1) % max(1, len(filtered))
            redraw = True
        elif ev == Screen.KEY_BACK and filtro:
            filtro = filtro[:-1]
            idx = 0
            scroll_offset = 0
            redraw = True
        elif ev in (10, 13) and filtered and filtered[0] != "[nenhum encontrado]":
            val = filtered[idx]
            log_debug(f"Usuário escolheu via autocomplete: {val}")
            return val
        elif isinstance(ev, int) and 32 <= ev < 127:
            ch = chr(ev)
            filtro += ch
            idx = 0
            scroll_offset = 0
            redraw = True
        if redraw:
            last_render = None

def tela_yesno(screen, question, cor=Screen.COLOUR_CYAN):
    lines = [question, "", "Use ← → para escolher, ENTER para confirmar."]
    options = ["Sim", "Não"]
    idx = 0
    needs_redraw = True
    while True:
        if needs_redraw:
            screen.clear()
            for y, line in enumerate(BANNER):
                screen.print_at(line, (screen.width - len(line)) // 2, y + 1, colour=BOX_COLOR, attr=Screen.A_BOLD)
            bbs_box(screen, lines, None)
            x = (screen.width - 18) // 2
            yb = (screen.height // 2) + 2
            for i, opt in enumerate(options):
                sel_attr = Screen.A_BOLD if i == idx else 0
                bg = BG_SELECT if i == idx else Screen.COLOUR_BLACK
                screen.print_at(f"[ {opt} ]", x + i*10, yb, colour=cor, bg=bg, attr=sel_attr)
            needs_redraw = False
        ev = screen.get_key()
        if ev == ord('<'):
            log_debug("Usuário saiu em pergunta Sim/Não")
            sys.exit(0)
        if ev == Screen.KEY_LEFT:
            idx = (idx - 1) % 2
            needs_redraw = True
        elif ev == Screen.KEY_RIGHT:
            idx = (idx + 1) % 2
            needs_redraw = True
        elif ev in (10, 13):
            log_debug(f"Usuário respondeu {options[idx]} para pergunta: {question}")
            return idx == 0

def tela_mensagem(screen, msg, cor=BORDER_COLOR):
    lines = [msg]
    needs_redraw = True
    while True:
        if needs_redraw:
            screen.clear()
            for y, line in enumerate(BANNER):
                screen.print_at(line, (screen.width - len(line)) // 2, y + 1, colour=BOX_COLOR, attr=Screen.A_BOLD)
            bbs_box(screen, lines, HELP_MSG)
            needs_redraw = False
        ev = screen.get_key()
        if ev is not None:
            return

def descobrir_argumentos_obrigatorios(script_path):
    # Lista apenas os argumentos do seu interesse, por exemplo "--perito"
    ARGS_DESEJADOS = {"--perito"}  # Adicione outros argumentos obrigatórios se quiser
    try:
        help_out = subprocess.run(
            [sys.executable, script_path, "--help"],
            capture_output=True, text=True
        )
        log_debug(f"Executando help para {script_path}")
        help_lines = help_out.stdout.splitlines()
        obrigatorios = {}
        for line in help_lines:
            line = line.strip()
            if not line or line.startswith("options:") or line.startswith("usage:"):
                continue
            # Pega argumentos "--algo ALGO"
            if line.startswith("--"):
                m = re.match(r"(--[a-zA-Z0-9_-]+)(\s+\S+)?\s*(.*)", line)
                if m:
                    argname = m.group(1)
                    helpmsg = m.group(3) if m.group(3) else ""
                    # Só entra se for argumento desejado!
                    if argname in ARGS_DESEJADOS:
                        obrigatorios[argname] = helpmsg.strip()
        log_debug(f"Argumentos obrigatórios detectados: {obrigatorios}")
        return obrigatorios
    except Exception as e:
        log_debug(f"Erro ao detectar argumentos obrigatórios: {e}")
        return {}

def descobrir_argumentos_opcionais(script_path):
    help_out = subprocess.run([sys.executable, script_path, "--help"], capture_output=True, text=True)
    help_lines = help_out.stdout.splitlines()
    args_set = set()
    for line in help_lines:
        match = re.match(r"\s*(--[a-zA-Z0-9_-]+)", line)
        if match:
            args_set.add(match.group(1))
    return args_set

def executar_script_estatistica(nome_script, di, df, extra_args, args_dict):
    script_path = os.path.join(GRAPHS_DIR, nome_script + ".py")
    cmd = [sys.executable, script_path, "--start", di, "--end", df] + extra_args
    for k, v in args_dict.items():
        cmd += [k, v]
    log_debug(f"Executando comando: {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    log_debug(f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")
    return proc

def executar_gerar_comentario(screen, nome_script, di, df, args_dict):
    proc = executar_script_estatistica(nome_script, di, df, ["--export-comment"], args_dict)
    if proc.returncode == 0:
        tela_mensagem(screen, "Comentário ChatGPT salvo no exports.", cor=Screen.COLOUR_GREEN)
    else:
        tela_mensagem(screen, "Erro ao gerar comentário!\n" + proc.stderr, cor=Screen.COLOUR_RED)

def sugerir_nomes_peritos():
    db_path = os.path.join("db", "atestmed.db")
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        # Só peritos que aparecem em pelo menos uma análise
        cur.execute("""
            SELECT DISTINCT p.nomePerito
            FROM peritos p
            JOIN analises a ON a.siapePerito = p.siapePerito
            WHERE p.nomePerito IS NOT NULL AND p.nomePerito != ''
            ORDER BY p.nomePerito
        """)
        nomes = [row[0] for row in cur.fetchall()]
        conn.close()
        return nomes
    except Exception as e:
        log_debug(f"Erro ao buscar nomes de peritos: {e}")
        return []

SUGGESTION_MAP = {
    "--perito": sugerir_nomes_peritos,
    "--nome-perito": sugerir_nomes_peritos,  # Se seus scripts usarem esse nome
}

def coletar_argumentos_universal(screen, obrigatorios):
    args_dict = {}
    for arg, helpmsg in obrigatorios.items():
        sug_func = SUGGESTION_MAP.get(arg)
        if sug_func:
            opcoes = sug_func()
            val = tela_autocomplete(screen, f"Selecione {arg[2:]}:", opcoes)
            if not val:
                log_debug(f"Usuário não selecionou valor para {arg}")
                break
            args_dict[arg] = val
        else:
            val = tela_input(screen, f"Preencha {arg}: {helpmsg}")
            if not val:
                log_debug(f"Usuário não preencheu argumento obrigatório {arg}")
                break
            args_dict[arg] = val
    return args_dict if len(args_dict) == len(obrigatorios) else None

def tela_selecao_csv(screen, files):
    idx = 0
    needs_redraw = True
    opcoes = ["Todos", "Um a um", "Cancelar"]
    while True:
        if needs_redraw:
            screen.clear()
            for y, line in enumerate(BANNER):
                screen.print_at(line, (screen.width - len(line)) // 2, y + 1, colour=BOX_COLOR, attr=Screen.A_BOLD)
            linhas = [f"{i+1}. {opt}" for i, opt in enumerate(opcoes)]
            bbs_box(screen, linhas, HELP_SUB, selected=idx)
            screen.print_at("Seleção de CSV".center(screen.width), 0, len(BANNER) + 1, colour=Screen.COLOUR_GREEN, attr=Screen.A_BOLD)
            needs_redraw = False
        ev = screen.get_key()
        if ev in (ord('q'), ord('Q')):
            sys.exit(0)
        if ev == Screen.KEY_DOWN:
            idx = (idx + 1) % len(opcoes)
            needs_redraw = True
        elif ev == Screen.KEY_UP:
            idx = (idx - 1) % len(opcoes)
            needs_redraw = True
        elif ev in (10, 13):
            return opcoes[idx]

# Função principal do TUI

def tela_selecao_csv(screen, files):
    idx = 0
    needs_redraw = True
    opcoes = ["Todos", "Um a um", "Cancelar"]
    while True:
        if needs_redraw:
            screen.clear()
            for y, line in enumerate(BANNER):
                screen.print_at(line,
                                (screen.width - len(line)) // 2,
                                y + 1,
                                colour=BOX_COLOR,
                                attr=Screen.A_BOLD)
            linhas = [f"{i+1}. {opt}" for i, opt in enumerate(opcoes)]
            bbs_box(screen, linhas, HELP_SUB, selected=idx)
            screen.print_at("Seleção de CSV".center(screen.width),
                            0, len(BANNER) + 1,
                            colour=Screen.COLOUR_GREEN,
                            attr=Screen.A_BOLD)
            needs_redraw = False
        ev = screen.get_key()
        if ev in (ord('q'), ord('Q')):
            sys.exit(0)
        if ev == Screen.KEY_DOWN:
            idx = (idx + 1) % len(opcoes)
            needs_redraw = True
        elif ev == Screen.KEY_UP:
            idx = (idx - 1) % len(opcoes)
            needs_redraw = True
        elif ev in (10, 13):  # Enter
            return opcoes[idx]

def tela_selecao_formatos(screen):
    opcoes = [
        ("Gerar Markdown (.md)", "--export-md"),
        ("Gerar Gráfico PNG (.png)", "--chart --export-png"),
        ("Gerar Comentário GPT", "--export-comment"),
    ]
    selecionados = [False] * len(opcoes)
    idx = 0
    needs_redraw = True
    while True:
        if needs_redraw:
            screen.clear()
            for y, line in enumerate(BANNER):
                screen.print_at(line, (screen.width - len(line)) // 2, y + 1, colour=BOX_COLOR, attr=Screen.A_BOLD)
            linhas = []
            for i, (label, _) in enumerate(opcoes):
                mark = "[X]" if selecionados[i] else "[ ]"
                sel = "→ " if i == idx else "  "
                linhas.append(f"{sel}{mark} {label}")
            linhas.append("")
            linhas.append("ESPACO: selecionar/deselecionar  ENTER: confirmar  < Voltar")
            bbs_box(screen, linhas, None, selected=idx)
            needs_redraw = False
        ev = screen.get_key()
        if ev == ord('<'):
            return None
        if ev == Screen.KEY_DOWN:
            idx = (idx + 1) % len(opcoes)
            needs_redraw = True
        elif ev == Screen.KEY_UP:
            idx = (idx - 1) % len(opcoes)
            needs_redraw = True
        elif ev == ord(' '):
            selecionados[idx] = not selecionados[idx]
            needs_redraw = True
        elif ev in (10, 13):  # ENTER
            if not any(selecionados):
                if tela_yesno(screen, "Nenhum formato selecionado. Cancelar geração?"):
                    return None
                else:
                    needs_redraw = True
                    continue
            return [opcoes[i][1] for i in range(len(opcoes)) if selecionados[i]]

# TUI principal

def main_bbs(screen):
    while True:
        opc = tela_menu(screen)

        # 0: Gerenciar Banco de Dados
        if opc == 0:
            # Desenha banner
            screen.clear()
            for y, line in enumerate(BANNER):
                screen.print_at(line,
                                (screen.width - len(line)) // 2,
                                y + 1,
                                colour=BOX_COLOR,
                                attr=Screen.A_BOLD)
            screen.refresh()

            # Recriar / Atualizar
            idx_bd = tela_submenu_estatisticas(screen, ["Recriar", "Atualizar"])
            if idx_bd is None:
                continue
            db_dir = "db"; db_path = os.path.join(db_dir, "atestmed.db")

            if idx_bd == 0:  # Recriar
                if tela_yesno(screen, "Excluir e recriar o banco?", cor=Screen.COLOUR_CYAN):
                    if os.path.exists(db_path): os.remove(db_path)
                    create_database(); calcular_indicadores(db_path)
                    tela_mensagem(screen, "✅ Banco recriado com sucesso!", cor=Screen.COLOUR_GREEN)
                continue

            # Atualizar: seleção de arquivos
            files = [f for f in os.listdir("data/raw") if f.lower().endswith(".csv")]
            modo = tela_selecao_csv(screen, files)
            if modo == "Cancelar":
                continue

            to_load = []
            if modo == "Todos":
                to_load = files
            else:
                # Um a um: adicionar, pular ou cancelar
                for f in files:
                    escolha = tela_submenu_estatisticas(
                        screen,
                        [f"Adicionar '{f}'", f"Pular '{f}'", "Cancelar"]
                    )
                    if escolha == 0:
                        to_load.append(f)
                    elif escolha == 2:
                        break

            for f in to_load:
                load_csv_to_db(os.path.join("data/raw", f), db_path)
            calcular_indicadores(db_path)
            tela_mensagem(screen, "📥 CSVs importados e indicadores atualizados!", cor=Screen.COLOUR_GREEN)

        # 1: Gráficos e Tabelas
        elif opc == 1:
            scripts = listar_scripts_estatisticas(GRAPHS_DIR)
            if not scripts:
                tela_mensagem(screen, "Nenhum script encontrado em graphs_and_tables!", cor=Screen.COLOUR_RED)
                continue
            idx_est = tela_submenu_estatisticas(screen, scripts)
            if idx_est is None: continue

            nome_script = scripts[idx_est]
            script_path = os.path.join(GRAPHS_DIR, nome_script + ".py")
            obrigatorios = descobrir_argumentos_obrigatorios(script_path)
            args_dict = coletar_argumentos_universal(screen, obrigatorios)
            if args_dict is None: continue

            di = tela_data(screen, "Digite data inicial (YYYY-MM-DD):")
            if not di: continue
            df = tela_data(screen, "Digite data final (YYYY-MM-DD):")
            if not df: continue

            # Nova seleção de formatos (multi seleção)
            formatos = tela_selecao_formatos(screen)
            if formatos is None:
                tela_mensagem(screen, "Operação cancelada pelo usuário.", cor=Screen.COLOUR_RED)
                continue

            for fmt in formatos:
                extra_args = fmt.split()
                proc = executar_script_estatistica(nome_script, di, df, extra_args, args_dict)
                if proc.returncode == 0:
                    if "--export-md" in extra_args:
                        tela_mensagem(screen, "Tabela Markdown gerada!", cor=Screen.COLOUR_GREEN)
                    elif "--export-png" in extra_args:
                        tela_mensagem(screen, "Gráfico PNG gerado!", cor=Screen.COLOUR_GREEN)
                    elif "--export-comment" in extra_args:
                        tela_mensagem(screen, "Comentário ChatGPT salvo no exports.", cor=Screen.COLOUR_GREEN)
                else:
                    tela_mensagem(screen, f"Erro ao executar: {fmt}\n" + proc.stderr, cor=Screen.COLOUR_RED)

        # 2: Análises em R
        elif opc == 2:
            scripts_r = listar_scripts_r()
            if not scripts_r:
                tela_mensagem(screen, "Nenhum script R encontrado em r_stats/scripts_r!", cor=Screen.COLOUR_RED)
                continue
            idx_r = tela_submenu_estatisticas(screen, scripts_r)
            if idx_r is None: continue

            nome_script_r = scripts_r[idx_r]
            script_path = os.path.join("r_stats", "scripts_r", nome_script_r)
            di = tela_data(screen, "Digite data inicial (YYYY-MM-DD):")
            if not di: continue
            df = tela_data(screen, "Digite data final (YYYY-MM-DD):")
            if not df: continue

            env = os.environ.copy()
            env["DATA_START"] = di
            env["DATA_END"] = df
            log_debug(f"Executando script R: {script_path} com DATA_START={di} e DATA_END={df}")
            proc = subprocess.run(
                ["Rscript", "-e", f'rmarkdown::render("{script_path}", params = list(arquivo = "dados_analise.csv"))'],
                capture_output=True, text=True, env=env
            )
            if proc.returncode == 0:
                tela_mensagem(screen, "✅ Script R executado com sucesso!", cor=Screen.COLOUR_GREEN)
            else:
                tela_mensagem(screen, f"❌ Erro ao executar script R:\n{proc.stderr}", cor=Screen.COLOUR_RED)

        # 3 ou None: Sair
        else:
            tela_mensagem(screen, "Saindo do sistema... Até logo!", cor=Screen.COLOUR_RED)
            log_debug("Usuário saiu do sistema")
            return

if __name__ == "__main__":
    log_debug("=== INICIANDO NOVA SESSÃO CLI TUI ===")
    Screen.wrapper(main_bbs)

