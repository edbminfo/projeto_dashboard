import configparser
import requests
import fdb
import psycopg2
import sys
import time
import os
import decimal
import binascii
import json
from datetime import datetime, date

# --- CONFIGURAÇÕES ---
print(">> Carregando Agente Sync v10.3 (ISO8859_1 Safe Mode)...", flush=True)
diretorio_base = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(diretorio_base, 'config.ini')
cursor_path = os.path.join(diretorio_base, 'cursor_controle.json')

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
    TAMANHO_LOTE = int(config['CONFIG'].get('tamanho_lote', 200))
except Exception as e:
    print(f"ERRO CRÍTICO NA CONFIG: {e}", flush=True); sys.exit(1)

# --- GERENCIAMENTO DE CURSOR ---
def carregar_cursores():
    if not os.path.exists(cursor_path): return {}
    try:
        with open(cursor_path, 'r') as f: return json.load(f)
    except: return {}

def salvar_cursor_novo(tabela, novo_id):
    cursores = carregar_cursores()
    id_atual = cursores.get(tabela)
    salvar = False
    if id_atual is None: salvar = True
    else:
        try:
            if int(novo_id) > int(id_atual): salvar = True
        except:
            if str(novo_id) > str(id_atual): salvar = True
    
    if salvar:
        cursores[tabela] = str(novo_id)
        with open(cursor_path, 'w') as f: json.dump(cursores, f, indent=4)

def get_ultimo_id(tabela):
    cursores = carregar_cursores()
    return cursores.get(tabela)

# --- CONEXÃO BANCO (A CORREÇÃO PRINCIPAL) ---
def get_connection():
    try:
        if DB_TIPO == 'FIREBIRD':
            # MUDANÇA: Usar 'ISO8859_1'. 
            # Isso mapeia cada byte para um char unicode. Nunca lança erro de decode.
            # Mesmo que o dado seja CP1252 sujo, ele vai ler como Latin1 e não vai travar.
            return fdb.connect(host=DB_HOST, port=int(DB_PORT), database=DB_PATH, user=DB_USER, password=DB_PASS, charset='ISO8859_1')
        elif DB_TIPO == 'POSTGRES':
            return psycopg2.connect(dbname=DB_PATH, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
    except Exception as e:
        print(f"   [FALHA CONEXÃO BANCO]: {e}", flush=True)
        return None

# --- UTILS ---
def limpar_valor(val):
    if val is None: return None
    
    # Se vier como bytes (alguns drivers antigos), tenta recuperar
    if isinstance(val, bytes):
        try: val = val.decode('cp1252', errors='replace')
        except: val = val.decode('iso-8859-1', errors='replace')

    # Limpeza de Strings
    if isinstance(val, str):
        # Remove NUL (0x00) que quebra o Postgres
        val = val.replace('\x00', '')
        
        # Opcional: Se quiser limpar o caractere fantasma 0x81 (que vira \x81 no ISO8859_1)
        # val = val.replace('\x81', '') 
        
        return val.strip()

    if isinstance(val, decimal.Decimal): return float(val)
    if isinstance(val, (datetime, date)): return val.isoformat()
    return val

def row_to_dict(row, col_names):
    data = {}
    for i, col in enumerate(col_names):
        key = col.lower().strip()
        val = limpar_valor(row[i])
        
        # Converte IDs para string
        campos_texto = ['id_grupo', 'id_secao', 'id_filial', 'id_cliente', 'id_vendedor', 'id_transportadora', 'id_formapag', 'tipo']
        if key in campos_texto and val is not None:
            val = str(val)
        
        data[key] = val
    
    if 'id' in data: data['id_original'] = str(data['id'])
    return data

def enviar_lote(endpoint, payload):
    try:
        url = f"{API_URL}{endpoint}"
        r = requests.post(url, json=payload, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=60)
        if r.status_code == 200: return True
        else: 
            print(f"   [ERRO API] {r.status_code}: {r.text}", flush=True)
            return False
    except Exception as e:
        print(f"   [ERRO POST]: {e}", flush=True); return False

# --- ENGINE UNIVERSAL ---
def sync_universal(nome_tabela, endpoint, tabela_sql, colunas_sql="*", campo_id="ID", filtro_extra="", usa_data=False):
    print(f"[{nome_tabela}]", end=" ", flush=True)
    
    ultimo_id = get_ultimo_id(nome_tabela)
    condicoes = []
    
    if ultimo_id: condicoes.append(f"{campo_id} > {ultimo_id}")
    elif usa_data: condicoes.append(f"DATA >= '{DATA_CORTE}'")

    if filtro_extra: condicoes.append(filtro_extra)
        
    where_clause = " WHERE " + " AND ".join(condicoes) if condicoes else ""
    limit = f"FIRST {TAMANHO_LOTE}" if DB_TIPO == 'FIREBIRD' else ""
    
    sql = f"SELECT {limit} {colunas_sql} FROM {tabela_sql} {where_clause} ORDER BY {campo_id} ASC"

    conn = get_connection()
    if not conn: return False
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
    except Exception as e:
        print(f"Erro SQL: {e}"); conn.close(); return False
    conn.close()

    if not rows:
        print("Sincronizado.", flush=True)
        return False

    payload = []
    maior_id_lote = ultimo_id
    
    for r in rows:
        d = row_to_dict(r, col_names)
        
        val_cursor = None
        col_cursor = campo_id.lower().split('.')[-1]
        if col_cursor in d: val_cursor = d[col_cursor]
        elif 'id' in d: val_cursor = d['id']
        elif 'id_original' in d: val_cursor = d['id_original']
             
        if val_cursor: maior_id_lote = val_cursor

        d.pop('rdb$db_key', None)
        payload.append(d)

    print(f"Enviando {len(payload)}... (Novo ID > {maior_id_lote})", end=" ", flush=True)
    
    if enviar_lote(endpoint, payload):
        try:
             if ultimo_id and (int(maior_id_lote) <= int(ultimo_id)): pass
        except: pass
        
        salvar_cursor_novo(nome_tabela, maior_id_lote)
        print("OK!", flush=True)
        return True
    else:
        print("Falha API.", flush=True)
        return False

# --- LOOP ---
if __name__ == "__main__":
    print(f"--- AGENTE SYNC INICIADO ---", flush=True)
    print(f"Controle: {cursor_path}")
    DATA_STR = DATA_CORTE.strftime('%Y-%m-%d')

    while True:
        try:
            dados_encontrados = False
            
            # Movimento
            if sync_universal("SAIDA", "/api/sync/saida", 
                              "SAIDA", "*", "ID", usa_data=True): dados_encontrados = True
            
            if sync_universal("SAIDA_PRODUTO", "/api/sync/saida_produto", 
                        "SAIDA_PRODUTO SP JOIN SAIDA S ON SP.ID_SAIDA = S.ID", 
                        "SP.*, S.ID", 
                        "S.ID", 
                        usa_data=False, 
                        filtro_extra=f"S.DATA >= '{DATA_STR}'"): dados_encontrados = True
            
            if sync_universal("SAIDA_FORMAPAG", "/api/sync/saida_formapag", 
                        "SAIDA_FORMAPAG SF JOIN SAIDA S ON SF.ID_SAIDA = S.ID", 
                        "SF.ID_SAIDA, SF.ID_FORMAPAG, SF.VALOR", 
                        "SF.ID_SAIDA", 
                        usa_data=False, 
                        filtro_extra=f"S.DATA >= '{DATA_STR}'"): dados_encontrados = True

            # Cadastros
            if sync_universal("PRODUTO", "/api/sync/cadastros/produto", "PRODUTO", 
                          "ID, NOME, PRECO_VENDA, CUSTO_TOTAL, ID_GRUPO, ATIVO", "ID"): dados_encontrados = True
            
            if sync_universal("CLIENTE", "/api/sync/cadastros/cliente", "PESSOA", 
                          "ID, NOME, CNPJ_CPF, CIDADE, ATIVO", "ID", filtro_extra="CLIENTE='S'"): dados_encontrados = True
            
            if sync_universal("VENDEDOR", "/api/sync/cadastros/vendedor", "VENDEDOR", 
                          "ID, NOME, COMISSAO, ATIVO", "ID"): dados_encontrados = True
            
            if sync_universal("GRUPO", "/api/sync/cadastros/grupo", "GRUPO", 
                          "ID, GRUPO AS NOME, ID_SECAO", "ID"): dados_encontrados = True
            
            if sync_universal("SECAO", "/api/sync/cadastros/secao", "SECAO", 
                          "ID, SECAO AS NOME", "ID"): dados_encontrados = True
            
            if sync_universal("FORMAPAG", "/api/sync/cadastros/formapag", "FORMAPAG", 
                          "ID, FORMAPAG AS NOME, TIPO", "ID"): dados_encontrados = True

            if not dados_encontrados:
                print(".", end="", flush=True)
                time.sleep(5)
            else:
                print("\n>> Próximo lote...", flush=True)
                time.sleep(0.1)
            
        except KeyboardInterrupt:
            print("\nParando...", flush=True); break
        except Exception as e:
            print(f"\n[ERRO LOOP]: {e}", flush=True); time.sleep(10)