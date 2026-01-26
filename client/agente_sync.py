import configparser
import requests
import fdb
import psycopg2
import sys
import time
import os
import decimal
from datetime import datetime, date

# --- CONFIGURAÇÕES ---
diretorio_base = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(diretorio_base, 'config.ini')
config = configparser.ConfigParser()
config.read(config_path)

try:
    API_URL = config['API']['url_base']
    TOKEN = config['API']['token_loja']
    DB_TIPO = config['DATABASE']['tipo'].upper()
    DB_PATH = config['DATABASE']['caminho']
    DB_USER = config['DATABASE']['usuario']
    DB_PASS = config['DATABASE']['senha']
    DB_HOST = config['DATABASE']['host']
    DB_PORT = config['DATABASE']['port']
    data_str = config['CONFIG']['data_corte']
    DATA_CORTE = datetime.strptime(data_str, "%d.%m.%Y").date()
    TAMANHO_LOTE = int(config['CONFIG'].get('tamanho_lote', 50))
except Exception as e:
    print(f"ERRO CRÍTICO: {e}"); sys.exit(1)

def get_connection():
    if DB_TIPO == 'FIREBIRD':
        return fdb.connect(host=DB_HOST, port=int(DB_PORT), database=DB_PATH, user=DB_USER, password=DB_PASS, charset='WIN1252')
    elif DB_TIPO == 'POSTGRES':
        return psycopg2.connect(dbname=DB_PATH, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)

def auto_setup():
    conn = get_connection(); cursor = conn.cursor()
    try:
        if DB_TIPO == 'FIREBIRD':
            cursor.execute("SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'SYNC_CONTROL'")
            if cursor.fetchone()[0] == 0:
                print("Criando tabela SYNC_CONTROL...")
                cursor.execute("CREATE TABLE SYNC_CONTROL (ID_ORIGINAL VARCHAR(50) NOT NULL, TABELA VARCHAR(30) NOT NULL, DATA_ENVIO TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (ID_ORIGINAL, TABELA))")
                conn.commit()
        elif DB_TIPO == 'POSTGRES':
            cursor.execute("CREATE TABLE IF NOT EXISTS sync_control (id_original VARCHAR(50) NOT NULL, tabela VARCHAR(30) NOT NULL, data_envio TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (id_original, tabela))")
            conn.commit()
    except: pass
    finally: conn.close()

def enviar_lote(endpoint, payload):
    try:
        # Timeout maior (60s) pois o ALTER TABLE no server pode demorar uns segundos
        r = requests.post(f"{API_URL}{endpoint}", json=payload, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=60)
        if r.status_code == 200: return True
        print(f"Erro API [{r.status_code}]: {r.text}")
        return False
    except Exception as e: print(f"Erro Conexão: {e}"); return False

def confirmar_envio(ids, tabela):
    conn = get_connection(); cursor = conn.cursor()
    sql = "INSERT INTO SYNC_CONTROL (ID_ORIGINAL, TABELA) VALUES (?, ?)" if DB_TIPO == 'FIREBIRD' else "INSERT INTO sync_control (id_original, tabela) VALUES (%s, %s)"
    try: cursor.executemany(sql, [(str(x), tabela) for x in ids]); conn.commit()
    except: pass
    finally: conn.close()

# --- HELPER: CONVERSÃO DE TIPOS E NOMES ---
def limpar_valor(val):
    """Converte tipos do Firebird incompatíveis com JSON"""
    if val is None: return None
    if isinstance(val, decimal.Decimal): return float(val)
    if isinstance(val, (datetime, date)): return val.isoformat()
    return val

def row_to_dict(row, col_names):
    """Mapeia linha do banco para Dicionário com chaves em minúsculo"""
    data = {}
    for i, col in enumerate(col_names):
        key = col.lower().strip() # Nome da coluna em minúsculo
        data[key] = limpar_valor(row[i])
    
    # O server exige 'id_original' para controle.
    # Se o banco local usa 'ID' ou 'id', copiamos para 'id_original'
    if 'id' in data:
        data['id_original'] = str(data['id'])
    
    return data

# --- HELPER SEGURO PARA CADASTROS ---
def safe_float(valor):
    if valor is None: return 0.0
    try: return float(str(valor).replace(',', '.'))
    except: return 0.0

# --- SYNC VENDAS (DINÂMICO/ESPELHO) ---
def sync_vendas():
    conn = get_connection(); 
    if not conn: return
    cursor = conn.cursor()
    
    ph = "?" if DB_TIPO == 'FIREBIRD' else "%s"
    
    # SELECT * PEGA TUDO AUTOMATICAMENTE
    sql = f"""
    SELECT FIRST {TAMANHO_LOTE} S.* FROM SAIDA S
    LEFT JOIN SYNC_CONTROL C ON S.ID = C.ID_ORIGINAL AND C.TABELA = 'SAIDA'
    WHERE C.ID_ORIGINAL IS NULL AND S.DATA >= {ph}
    ORDER BY S.DATA
    """
    if DB_TIPO == 'POSTGRES': sql = sql.replace(f"FIRST {TAMANHO_LOTE}", "").replace("?", "%s") + f" LIMIT {TAMANHO_LOTE}"

    try:
        cursor.execute(sql, (DATA_CORTE,))
        rows = cursor.fetchall()
        # Captura os nomes das colunas da consulta
        col_names = [desc[0] for desc in cursor.description]
    except Exception as e:
        print(f"Erro ao ler Vendas: {e}"); conn.close(); return

    conn.close()
    if not rows: return

    payload = []
    ids = []
    
    for r in rows:
        # Transforma em dict dinâmico
        d = row_to_dict(r, col_names)
        
        # Só adiciona se tiver conseguido mapear o ID
        if 'id_original' in d:
            payload.append(d)
            ids.append(d['id_original'])

    print(f"Enviando {len(payload)} vendas... (Detectadas {len(col_names)} colunas)")

    if enviar_lote("/api/sync/vendas", payload):
        confirmar_envio(ids, "SAIDA")
        print(f"[Vendas] Sucesso.")

# --- SYNC CADASTROS (ESTRUTURADO) ---
# Mappers manuais para cadastros
def map_cli(r): return {"id_original": str(r[0]), "nome": str(r[1]), "cpf_cnpj": str(r[2]), "cidade": str(r[3]), "ativo": str(r[4])}
def map_prod(r): return {"id_original": str(r[0]), "nome": str(r[1]), "preco_venda": safe_float(r[2]), "custo_total": safe_float(r[3]), "id_grupo": str(r[4]), "ativo": str(r[5])}
def map_grp(r): return {"id_original": str(r[0]), "nome": str(r[1]), "id_secao": str(r[2])}
def map_sec(r): return {"id_original": str(r[0]), "nome": str(r[1])}
def map_vend(r): return {"id_original": str(r[0]), "nome": str(r[1]), "comissao": safe_float(r[2]), "ativo": str(r[3])}

def sync_gen(nome, endp, sql, mapper):
    conn = get_connection(); cursor = conn.cursor()
    try: cursor.execute(sql); rows = cursor.fetchall()
    except: conn.close(); return
    conn.close()
    if not rows: return
    p = []; i = []
    for r in rows:
        try:
            o = mapper(r)
            if o: p.append(o); i.append(o["id_original"])
        except: pass
    if p and enviar_lote(f"/api/sync/cadastros/{endp}", p):
        confirmar_envio(i, nome); print(f"[{nome}] {len(i)} ok")

if __name__ == "__main__":
    print("--- AGENTE REPLICAÇÃO DINÂMICA INICIADO ---")
    auto_setup()
    while True:
        try:
            sync_vendas()
            
            # Cadastros
            sync_gen("CLIENTE", "cliente", f"SELECT FIRST {TAMANHO_LOTE} ID, NOME, CNPJ_CPF, CIDADE, ATIVO FROM PESSOA WHERE CLIENTE='S' AND ID NOT IN (SELECT ID_ORIGINAL FROM SYNC_CONTROL WHERE TABELA='CLIENTE')", map_cli)
            
            # Vendedor com LEFT JOIN para segurança
            sql_vend = f"SELECT FIRST {TAMANHO_LOTE} V.ID, V.NOME, V.COMISSAO, V.ATIVO FROM VENDEDOR V LEFT JOIN SYNC_CONTROL C ON V.ID=C.ID_ORIGINAL AND C.TABELA='VENDEDOR' WHERE C.ID_ORIGINAL IS NULL"
            sync_gen("VENDEDOR", "vendedor", sql_vend, map_vend)

            sync_gen("PRODUTO", "produto", f"SELECT FIRST {TAMANHO_LOTE} ID, NOME, PRECO_VENDA, CUSTO_TOTAL, ID_GRUPO, ATIVO FROM PRODUTO WHERE ID NOT IN (SELECT ID_ORIGINAL FROM SYNC_CONTROL WHERE TABELA='PRODUTO')", map_prod)
            sync_gen("GRUPO", "grupo", f"SELECT FIRST {TAMANHO_LOTE} ID, GRUPO, ID_SECAO FROM GRUPO WHERE ID NOT IN (SELECT ID_ORIGINAL FROM SYNC_CONTROL WHERE TABELA='GRUPO')", map_grp)
            sync_gen("SECAO", "secao", f"SELECT FIRST {TAMANHO_LOTE} ID, SECAO FROM SECAO WHERE ID NOT IN (SELECT ID_ORIGINAL FROM SYNC_CONTROL WHERE TABELA='SECAO')", map_sec)

            time.sleep(5)
        except KeyboardInterrupt: break
        except Exception as e: print(f"Erro Ciclo: {e}"); time.sleep(10)