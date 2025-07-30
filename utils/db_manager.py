#!/usr/bin/env python3
import os
import sqlite3
import pandas as pd
import inquirer

# ------------------------
# Funções de Acesso ao DB
# ------------------------

def create_database():
    """
    Cria o diretório db (se não existir) e as tabelas necessárias no SQLite.
    """
    db_dir = './db'
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        print("📂 Pasta 'db' criada com sucesso!")
    db_path = os.path.join(db_dir, 'atestmed.db')

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Criação das tabelas
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS peritos (
            siapePerito INTEGER PRIMARY KEY,
            nomePerito TEXT NOT NULL,
            cpfPerito INTEGER,
            lotacao TEXT,
            cr TEXT,
            dr TEXT
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS protocolos (
            protocolo INTEGER PRIMARY KEY,
            nomePerito TEXT NOT NULL,
            cpfCidadao INTEGER,
            sigla TEXT,
            tipoComunicacao TEXT,
            dataComunicacao TEXT,
            dataConclusao TEXT,
            motivo TEXT,
            tipoAfastamento TEXT,
            FOREIGN KEY (nomePerito) REFERENCES peritos (nomePerito)
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS analises (
            protocolo INTEGER PRIMARY KEY,
            dataHoraIniPericia TEXT,
            dataHoraFimPericia TEXT,
            duracaoPericia TEXT,
            motivoNaoConformado REAL,
            tipoPrazoAfastamento REAL,
            totalDiasRepouso REAL,
            siapePerito INTEGER,
            FOREIGN KEY (protocolo) REFERENCES protocolos (protocolo),
            FOREIGN KEY (siapePerito) REFERENCES peritos (siapePerito)
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS motivos_nc (
            motivoNaoConformado REAL PRIMARY KEY,
            descricaoMotivo TEXT
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS afastamentos (
            tipoAfastamento TEXT PRIMARY KEY,
            descricaoAfastamento TEXT
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS indicadores (
            perito INTEGER PRIMARY KEY,
            icra REAL,
            iatd REAL,
            scoreFinal REAL,
            FOREIGN KEY (perito) REFERENCES peritos (siapePerito)
        );
    ''')

    conn.commit()
    conn.close()
    print("✅ Banco de dados criado com sucesso!")


def load_csv_to_db(csv_path, db_path):
    """
    Carrega os dados de um CSV para as tabelas peritos, protocolos e analises.
    Usa INSERT OR REPLACE para evitar duplicatas.
    """
    df = pd.read_csv(csv_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for row in df.itertuples(index=False):
        cursor.execute('''
            INSERT OR REPLACE INTO peritos (
                siapePerito, nomePerito, cpfPerito, lotacao, cr, dr
            ) VALUES (?, ?, ?, ?, ?, ?)''', (
            getattr(row, 'siapePerito', None),
            getattr(row, 'nomePerito', None),
            getattr(row, 'cpfPerito', None),
            getattr(row, 'lotacao', None),
            getattr(row, 'cr', None),
            getattr(row, 'dr', None)
        ))
        cursor.execute('''
            INSERT OR REPLACE INTO protocolos (
                protocolo, nomePerito, cpfCidadao, sigla,
                tipoComunicacao, dataComunicacao, dataConclusao,
                motivo, tipoAfastamento
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
            getattr(row, 'protocolo', None),
            getattr(row, 'nomePerito', None),
            getattr(row, 'cpfCidadao', None),
            getattr(row, 'sigla', None),
            getattr(row, 'tipoComunicacao', None),
            getattr(row, 'dataComunicacao', None),
            getattr(row, 'dataConclusao', None),
            getattr(row, 'motivo', None),
            getattr(row, 'tipoAfastamento', None)
        ))
        cursor.execute('''
            INSERT OR REPLACE INTO analises (
                protocolo, dataHoraIniPericia, dataHoraFimPericia,
                duracaoPericia, motivoNaoConformado,
                tipoPrazoAfastamento, totalDiasRepouso, siapePerito
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (
            getattr(row, 'protocolo', None),
            getattr(row, 'dataHoraIniPericia', None),
            getattr(row, 'dataHoraFimPericia', None),
            getattr(row, 'duracaoPericia', None),
            getattr(row, 'motivoNaoConformado', None),
            getattr(row, 'tipoPrazoAfastamento', None),
            getattr(row, 'totalDiasRepouso', None),
            getattr(row, 'siapePerito', None)
        ))

    conn.commit()
    conn.close()
    print(f"📥 Dados do arquivo '{os.path.basename(csv_path)}' carregados com sucesso!")


def calcular_indicadores(db_path):
    """
    Calcula ICRA, IATD e Score Final para cada perito com base nos critérios definidos.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT siapePerito,
               COUNT(*) AS total_analises,
               SUM(CASE WHEN motivoNaoConformado = 1 THEN 1 ELSE 0 END) AS nc_count,
               AVG((julianday(dataHoraFimPericia) - julianday(dataHoraIniPericia)) * 86400) AS duracao_media_segundos,
               SUM(CASE WHEN (julianday(dataHoraFimPericia) - julianday(dataHoraIniPericia)) * 86400 <= 15 THEN 1 ELSE 0 END) AS count_15s
        FROM analises
        GROUP BY siapePerito;
    ''')
    rows = cursor.fetchall()

    cursor.execute('''
        SELECT AVG(nc_count * 1.0 / total_analises) FROM (
            SELECT COUNT(*) AS total_analises,
                   SUM(CASE WHEN motivoNaoConformado = 1 THEN 1 ELSE 0 END) AS nc_count
            FROM analises
            GROUP BY siapePerito
        );
    ''')
    media_nc = cursor.fetchone()[0] or 0

    for siape, total, nc_count, duracao_media, count_15s in rows:
        icra = 0.0
        produtividade = total / ((duracao_media * total) / 3600) if duracao_media and total else 0
        if produtividade >= 50:
            icra += 3.0
        if count_15s >= 10:
            icra += 2.0
        # sobreposição temporal não implementada
        if (nc_count * 1.0 / total) >= 2 * media_nc:
            icra += 1.0

        iatd_raw = 1 - (nc_count * 1.0 / total) if total else 0
        iatd = iatd_raw

        score_final = icra + (1 - iatd)

        cursor.execute('''
            INSERT OR REPLACE INTO indicadores (perito, icra, iatd, scoreFinal)
            VALUES (?, ?, ?, ?);
        ''', (siape, icra, iatd, score_final))

    conn.commit()
    conn.close()
    print("🔢 Indicadores (ICRA, IATD, Score) calculados e atualizados com sucesso.")


# ------------------------
# Fluxo Principal
# ------------------------

def process_database():
    db_dir = './db'
    db_path = os.path.join(db_dir, 'atestmed.db')

    if not os.path.exists(db_path):
        q1 = [inquirer.List('opt', message="O banco não foi encontrado. Deseja criar?",
                             choices=['✅ Sim, criar', '❌ Não, sair'], carousel=True)]
        a1 = inquirer.prompt(q1)
        if a1['opt'].startswith('✅'):
            create_database()
            calcular_indicadores(db_path)
            q2 = [inquirer.List('opt2', message="Carregar CSVs de 'data/raw'...",
                                  choices=['📂 Todos', '📄 Um a um'], carousel=True)]
            a2 = inquirer.prompt(q2)
            files = [f for f in os.listdir('./data/raw') if f.lower().endswith('.csv')]
            if a2['opt2'].startswith('📂'):
                for f in files:
                    load_csv_to_db(os.path.join('./data/raw', f), db_path)
            else:
                for f in files:
                    qf = [inquirer.Confirm('c', message=f"Carregar '{f}'?", default=True)]
                    af = inquirer.prompt(qf)
                    if af['c']:
                        load_csv_to_db(os.path.join('./data/raw', f), db_path)
            calcular_indicadores(db_path)
        else:
            print("👋 Operação cancelada.")
    else:
        q3 = [inquirer.List('opt3', message="Banco já existe. O que deseja fazer?",
                             choices=['🔄 Recriar', '♻️ Atualizar'], carousel=True)]
        a3 = inquirer.prompt(q3)
        if a3['opt3'].startswith('🔄'):
            qc = [inquirer.Confirm('c', message="Tem certeza que quer excluir e recriar?", default=False)]
            ac = inquirer.prompt(qc)
            if ac['c']:
                os.remove(db_path)
                create_database()
                # Novo: após recriar, importar CSVs
                q4 = [inquirer.List('opt4', message="Carregar CSVs de 'data/raw'...",
                                     choices=['📂 Todos', '📄 Um a um'], carousel=True)]
                a4 = inquirer.prompt(q4)
                files = [f for f in os.listdir('./data/raw') if f.lower().endswith('.csv')]
                if a4['opt4'].startswith('📂'):
                    for f in files:
                        load_csv_to_db(os.path.join('./data/raw', f), db_path)
                else:
                    for f in files:
                        qf = [inquirer.Confirm('c', message=f"Carregar '{f}'?", default=True)]
                        af = inquirer.prompt(qf)
                        if af['c']:
                            load_csv_to_db(os.path.join('./data/raw', f), db_path)
                calcular_indicadores(db_path)
            else:
                print("👋 Operação cancelada.")
        else:
            q5 = [inquirer.List('opt5', message="Atualizar com dados novos: carregar CSVs?",
                                 choices=['📂 Todos', '📄 Um a um'], carousel=True)]
            a5 = inquirer.prompt(q5)
            files = [f for f in os.listdir('./data/raw') if f.lower().endswith('.csv')]
            if a5['opt5'].startswith('📂'):
                for f in files:
                    load_csv_to_db(os.path.join('./data/raw', f), db_path)
            else:
                for f in files:
                    qf = [inquirer.Confirm('c', message=f"Carregar '{f}'?", default=True)]
                    af = inquirer.prompt(qf)
                    if af['c']:
                        load_csv_to_db(os.path.join('./data/raw', f), db_path)
            calcular_indicadores(db_path)

if __name__ == '__main__':
    process_database()

