import configparser
import requests
import fdb
import sys
import time
import os
import decimal
from datetime import datetime, date, time as dt_time

# --- CONFIGURAÇÕES DE PERFORMANCE ---
DELAY_OCIOSO = 30       # Segundos quando não há nada para enviar
DELAY_ENTRE_LOTES = 1   # Segundos entre envios para não sobrecarregar
TIMEOUT_API = 60        # Timeout da requisição

# --- VERSÃO ---
VERSAO = "49.0 (Filtros removidos - Tratamento no Backend)"

print(f">> Iniciando Agente Sync v{VERSAO}...", flush=True)

# --- CARREGAMENTO CONFIG ---
diretorio_base = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(diretorio_base, 'config.ini')
config = configparser.ConfigParser()

try:
    config.read(config_path)
    API_URL = config.get('API', 'url_base', fallback="https://api-dash.bmhelp.click")
    TAMANHO_LOTE = config.getint('CONFIG', 'tamanho_lote', fallback=50)
    TOKEN = config.get('API', 'token_loja', fallback=None)
    
    # Data de Corte (Padrão 2026-01-01)
    DATA_CORTE = config.get('CONFIG', 'data_corte', fallback="2026-01-01")
    
    # Configurações de Banco
    DB_SEC = 'DATABASE' if config.has_section('DATABASE') else 'DB'
    DB_PATH = config.get(DB_SEC, 'path' if DB_SEC=='DB' else 'caminho')
    DB_USER = config.get(DB_SEC, 'user' if DB_SEC=='DB' else 'usuario')
    DB_PASS = config.get(DB_SEC, 'pass' if DB_SEC=='DB' else 'senha')
    DB_HOST = config.get(DB_SEC, 'host', fallback="localhost")
    DB_PORT = config.get(DB_SEC, 'port', fallback="3050")

    if not TOKEN: raise Exception("Token (token_loja) não configurado.")
except Exception as e:
    print(f"ERRO CONFIG: {e}")
    sys.exit(1)

def get_connection():
    try:
        return fdb.connect(
            host=DB_HOST, port=int(DB_PORT), database=DB_PATH, 
            user=DB_USER, password=DB_PASS, charset='ISO8859_1'
        )
    except Exception as e:
        print(f"   [FALHA BANCO]: {e}", flush=True)
        return None

# --- TABELAS PARA SINCRONISMO (UPSERT) ---
TABELAS_SYNC = [
    {"nome": "USUARIOS", "endpoint": "/api/sync/cadastros/usuario_pdv", "sql": "ID, NOME, LOGIN"},
    {"nome": "SECAO", "endpoint": "/api/sync/cadastros/secao", "sql": "ID, SECAO AS NOME"},
    {"nome": "GRUPO", "endpoint": "/api/sync/cadastros/grupo", "sql": "ID, GRUPO AS NOME, ID_SECAO"},
    {"nome": "FAMILIA", "endpoint": "/api/sync/cadastros/familia", "sql": "ID, FAMILIA AS NOME"},
    {"nome": "VENDEDOR", "endpoint": "/api/sync/cadastros/vendedor", "sql": "ID, NOME, COMISSAO, ATIVO"},
    {"nome": "FABRICANTE", "endpoint": "/api/sync/cadastros/fabricante", "sql": "ID, FABRICANTE AS NOME"},
    {"nome": "PESSOA", "endpoint": "/api/sync/cadastros/cliente", "sql": "ID, NOME, CNPJ_CPF, CIDADE, ATIVO"},
    {"nome": "FORMAPAG", "endpoint": "/api/sync/cadastros/formapag", "sql": "ID, FORMAPAG AS NOME, TIPO"},
    {"nome": "PRODUTO", "endpoint": "/api/sync/cadastros/produto", "sql": "ID, NOME, PRECO_VENDA, CUSTO_TOTAL, ID_GRUPO, ID_FABRICANTE, ID_FORNECEDOR, ID_FAMILIA, ATIVO"},
    
    # MOVIMENTAÇÃO (Campos ELIMINADO e NORMAL mantidos para filtro no Backend)
    {"nome": "SAIDA", "endpoint": "/api/sync/saida", "sql": "ID, ID_FILIAL, DATA, HORA, TOTAL, ID_CLIENTE, TERMINAL, USUARIO AS ID_USUARIO, ELIMINADO, NORMAL, NUMERO, SERIE, TIPOSAIDA, TIPO, CHAVENFE"},
    {"nome": "SAIDA_PRODUTO", "endpoint": "/api/sync/saida_produto", "sql": "ID_SAIDA, ID_PRODUTO, ID_VENDEDOR, QUANT, TOTAL"},
    {"nome": "SAIDA_FORMAPAG", "endpoint": "/api/sync/saida_formapag", "sql": "ID_SAIDA, ID_FORMAPAG, VALOR"},
]

def limpar_valor(val):
    if val is None: return None
    if isinstance(val, bytes):
        try: return val.hex()
        except: return str(val)
    if isinstance(val, str): return val.replace('\x00', '').strip()
    if isinstance(val, decimal.Decimal): return float(val)
    if isinstance(val, (datetime, date, dt_time)): return val.isoformat()
    return val

def row_to_dict(row, col_names, db_key):
    data = {}
    campos_numericos = ['total', 'quant', 'valor', 'preco_venda', 'custo_total', 'comissao']
    campos_string = ['id', 'id_filial', 'id_cliente', 'id_usuario', 'id_saida', 'id_produto', 'id_formapag', 'terminal', 'numero', 'serie', 'chavenfe', 'id_vendedor']

    for i, col in enumerate(col_names):
        key = col.lower().strip()
        val = limpar_valor(row[i])
        
        if key in campos_numericos:
            if isinstance(val, str): val = val.replace(',', '.')
            if val is not None: val = str(val) 
        
        if key in campos_string and val is not None:
            val = str(val)
            
        data[key] = val
    
    if 'id' in data:
        data['id_original'] = data['id']
        del data['id']
    else:
        data['id_original'] = limpar_valor(db_key)
        
    return data

def enviar_lote(endpoint, payload, tabela_origem, db_keys):
    try:
        headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
        r = requests.post(f"{API_URL}{endpoint}", json=payload, headers=headers, timeout=TIMEOUT_API)

        if r.status_code == 200:
            conn = get_connection()
            if conn:
                cursor = conn.cursor()
                cursor.executemany(f"UPDATE {tabela_origem} SET SYNK_DASH_PEND = 'N' WHERE RDB$DB_KEY = ?", [(x,) for x in db_keys])
                conn.commit()
                conn.close()
                return True
        else:
            print(f"\n   [ERRO API {r.status_code}]: {r.text[:150]}")
    except Exception as e:
        print(f"\n   [ERRO ENVIO]: {e}")
    return False

def executar_ciclo_sync():
    encontrou_dados = False
    hora_atual = datetime.now().strftime("%H:%M:%S")

    for config_tbl in TABELAS_SYNC:
        conn = get_connection()
        if not conn: return False
        cursor = conn.cursor()
        tabela = config_tbl['nome']
        
        try:
            sql = f"SELECT FIRST {TAMANHO_LOTE} RDB$DB_KEY, {config_tbl['sql']}, SYNK_DASH_PEND FROM {tabela} WHERE SYNK_DASH_PEND = 'S'"
            
            # Filtro simplificado apenas por DATA_CORTE
            if tabela == "SAIDA":
                sql += f" AND DATA >= '{DATA_CORTE}'"
            
            elif tabela in ["SAIDA_PRODUTO", "SAIDA_FORMAPAG"]:
                sql += f""" AND EXISTS (
                    SELECT 1 FROM SAIDA 
                    WHERE SAIDA.ID = {tabela}.ID_SAIDA 
                    AND SAIDA.DATA >= '{DATA_CORTE}'
                )"""

            cursor.execute(sql)
            rows = cursor.fetchall()
            col_names = [d[0] for d in cursor.description][1:-1]
            conn.close()

            if rows:
                encontrou_dados = True
                payload = [row_to_dict(r[1:-1], col_names, r[0]) for r in rows]
                db_keys = [r[0] for r in rows]

                print(f"[{hora_atual}] Sincronizando {len(payload)} registros de {tabela}...", end=" ", flush=True)
                if enviar_lote(config_tbl['endpoint'], payload, tabela, db_keys):
                    print("✅")
                else:
                    print("❌")
                time.sleep(DELAY_ENTRE_LOTES)
        except Exception as e:
            if conn: conn.close()
            print(f"Erro ao ler {tabela}: {e}")
    return encontrou_dados

def configurar_estrutura_banco():
    print(f"\n--- Agente Sync Dashboard v{VERSAO} ---")
    conn = get_connection()
    if not conn: return
    cursor = conn.cursor()

    for t in TABELAS_SYNC:
        tbl = t['nome']
        try:
            cursor.execute(f"SELECT FIRST 1 SYNK_DASH_PEND FROM {tbl}")
        except:
            print(f">> Configurando infraestrutura em {tbl}...")
            try:
                cursor.execute(f"ALTER TABLE {tbl} ADD SYNK_DASH_PEND CHAR(1) DEFAULT 'S'")
                conn.commit()
                cursor.execute(f"""
                    CREATE OR ALTER TRIGGER TG_SYNC_{tbl[:20]} FOR {tbl}
                    ACTIVE BEFORE INSERT OR UPDATE POSITION 99
                    AS BEGIN
                        IF (NEW.SYNK_DASH_PEND IS NULL OR NEW.SYNK_DASH_PEND = OLD.SYNK_DASH_PEND OR INSERTING) THEN
                            NEW.SYNK_DASH_PEND = 'S';
                    END
                """)
                conn.commit()
            except: pass

    print(f">> Manutenção de integridade (Data Corte: {DATA_CORTE})")
    # Agora limpa apenas o que for anterior à data de corte
    cursor.execute(f"UPDATE SAIDA SET SYNK_DASH_PEND = 'N' WHERE DATA < '{DATA_CORTE}' AND SYNK_DASH_PEND = 'S'")
    
    sql_limpeza = f"""
        UPDATE {{tabela}} SET SYNK_DASH_PEND = 'N' 
        WHERE SYNK_DASH_PEND = 'S' 
        AND NOT EXISTS (
            SELECT 1 FROM SAIDA 
            WHERE SAIDA.ID = {{tabela}}.ID_SAIDA 
            AND SAIDA.DATA >= '{DATA_CORTE}'
        )
    """
    cursor.execute(sql_limpeza.format(tabela="SAIDA_PRODUTO"))
    cursor.execute(sql_limpeza.format(tabela="SAIDA_FORMAPAG"))
    
    for t in TABELAS_SYNC:
        cursor.execute(f"UPDATE {t['nome']} SET SYNK_DASH_PEND = 'S' WHERE SYNK_DASH_PEND IS NULL")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    configurar_estrutura_banco()
    print(f"Sincronismo Ativo. Pressione CTRL+C para encerrar.\n")

    while True:
        try:
            # Função verificar_delecoes removida; agora o Backend cuida disso
            if not executar_ciclo_sync():
                sys.stdout.write(".")
                sys.stdout.flush()
                time.sleep(DELAY_OCIOSO)
            else:
                time.sleep(DELAY_ENTRE_LOTES)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"\n[ERRO CRÍTICO]: {e}")
            time.sleep(10)