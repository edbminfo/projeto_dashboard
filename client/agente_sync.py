import configparser
import requests
import fdb
import sys
import time
import os
import decimal
from datetime import datetime, date

# --- CONSTANTES DE CONTROLE ---
DELAY_OCIOSO = 30       
DELAY_ENTRE_LOTES = 2   

# --- CARREGAMENTO DE CONFIGURAÇÕES ---
print(">> Carregando Agente Sync v21.5 (Estrutura Firebird Corrigida)...", flush=True)
diretorio_base = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(diretorio_base, 'config.ini')

config = configparser.ConfigParser()
try:
    config.read(config_path)
    if not config.sections(): raise Exception("Arquivo config.ini vazio ou inválido.")
    
    # Defaults
    API_URL = config.get('API', 'url_base', fallback="http://127.0.0.1:8000")
    TAMANHO_LOTE = config.getint('CONFIG', 'tamanho_lote', fallback=20)
    
    # Obrigatórios
    TOKEN = config.get('API', 'token_loja', fallback=None)
    if not TOKEN: raise Exception("Token da loja não encontrado.")
    
    DB_PATH = config.get('DATABASE', 'caminho')
    DB_USER = config.get('DATABASE', 'usuario')
    DB_PASS = config.get('DATABASE', 'senha')
    DB_HOST = config.get('DATABASE', 'host')
    DB_PORT = config.get('DATABASE', 'port')

except Exception as e:
    print(f"ERRO DE CONFIGURAÇÃO: {e}"); sys.exit(1)

# --- CONEXÃO BANCO ---
def get_connection():
    try:
        return fdb.connect(host=DB_HOST, port=int(DB_PORT), database=DB_PATH, user=DB_USER, password=DB_PASS, charset='ISO8859_1')
    except Exception as e:
        print(f"   [FALHA CONEXÃO BANCO]: {e}", flush=True); return None

# --- INSTALADOR AUTOMÁTICO ---
def verificar_e_configurar_banco():
    conn = get_connection()
    if not conn: sys.exit(1)
    cursor = conn.cursor()

    # Lista completa de tabelas para criar o campo SYNC_DASH
    tabelas_cadastro = ['PRODUTO', 'PESSOA', 'VENDEDOR', 'GRUPO', 'SECAO', 'FORMAPAG', 'FABRICANTE']
    tabelas_movimento = ['SAIDA', 'SAIDA_PRODUTO', 'SAIDA_FORMAPAG']
    todas_tabelas = tabelas_cadastro + tabelas_movimento

    # Verifica se já está instalado (testando na tabela PRODUTO)
    precisa_instalar = False
    try: cursor.execute("SELECT FIRST 1 SYNC_DASH FROM PRODUTO"); conn.commit()
    except: precisa_instalar = True

    # Se já tem em produto, verifica se as tabelas novas (FABRICANTE/FORMAPAG) também têm
    if not precisa_instalar:
        for tbl_nova in ['FABRICANTE', 'FORMAPAG']:
            try: 
                cursor.execute(f"SELECT FIRST 1 SYNC_DASH FROM {tbl_nova}")
                conn.commit()
            except:
                print(f">> Atualizando estrutura da tabela {tbl_nova}...", flush=True)
                try:
                    cursor.execute(f"ALTER TABLE {tbl_nova} ADD SYNC_DASH CHAR(1) DEFAULT 'N'")
                    cursor.execute(f"""
                        CREATE OR ALTER TRIGGER TG_SYNC_{tbl_nova} FOR {tbl_nova}
                        ACTIVE BEFORE INSERT OR UPDATE POSITION 0
                        AS BEGIN
                            IF (NEW.SYNC_DASH IS DISTINCT FROM 'S') THEN NEW.SYNC_DASH = 'N';
                        END
                    """)
                    conn.commit()
                except Exception as e: print(f"Erro ao atualizar {tbl_nova}: {e}")
        
        print(">> Banco verificado.", flush=True); conn.close(); return

    print("\n" + "="*50)
    print(" DETECTADA PRIMEIRA EXECUÇÃO - CONFIGURANDO BANCO")
    print("="*50)
    
    while True:
        data_input = input(">> A partir de qual data deseja sincronizar as VENDAS? (dd/mm/aaaa): ")
        try:
            data_corte = datetime.strptime(data_input, "%d/%m/%Y").date()
            data_str = data_corte.strftime("%Y-%m-%d")
            break
        except: print("Data inválida.")

    print("\n1. Criando colunas SYNC_DASH...")
    for tbl in todas_tabelas:
        try:
            cursor.execute(f"ALTER TABLE {tbl} ADD SYNC_DASH CHAR(1) DEFAULT 'N'")
            conn.commit()
        except: pass

    print("\n2. Criando Triggers...")
    for tbl in todas_tabelas:
        sql_trigger = f"""
        CREATE OR ALTER TRIGGER TG_SYNC_{tbl} FOR {tbl}
        ACTIVE BEFORE INSERT OR UPDATE POSITION 0
        AS BEGIN
            IF (NEW.SYNC_DASH IS DISTINCT FROM 'S') THEN NEW.SYNC_DASH = 'N';
        END
        """
        try: cursor.execute(sql_trigger); conn.commit()
        except Exception as e: print(f"Erro Trigger {tbl}: {e}")

    print(f"\n3. Marcando registros (Corte: {data_input})...")
    for tbl in tabelas_cadastro:
        try:
            cursor.execute(f"UPDATE {tbl} SET SYNC_DASH = 'N'")
            conn.commit()
        except Exception as e: print(f"Erro update inicial {tbl}: {e}")

    cursor.execute(f"UPDATE SAIDA SET SYNC_DASH = 'N' WHERE DATA >= '{data_str}'")
    cursor.execute(f"UPDATE SAIDA_PRODUTO SP SET SYNC_DASH = 'N' WHERE EXISTS (SELECT 1 FROM SAIDA S WHERE S.ID = SP.ID_SAIDA AND S.DATA >= '{data_str}')")
    cursor.execute(f"UPDATE SAIDA_FORMAPAG SF SET SYNC_DASH = 'N' WHERE EXISTS (SELECT 1 FROM SAIDA S WHERE S.ID = SF.ID_SAIDA AND S.DATA >= '{data_str}')")
    
    conn.commit(); conn.close()
    print(">> Configuração OK! Iniciando...")
    time.sleep(2)

# --- UTILS ---
def limpar_valor(val):
    if val is None: return None
    if isinstance(val, bytes):
        try: val = val.decode('cp1252', errors='replace')
        except: val = val.decode('iso-8859-1', errors='replace')
    if isinstance(val, str): return val.replace('\x00', '').strip()
    if isinstance(val, decimal.Decimal): return float(val)
    if isinstance(val, (datetime, date)): return val.isoformat()
    return val

def row_to_dict(row, col_names):
    data = {}
    for i, col in enumerate(col_names):
        key = col.lower().strip()
        val = limpar_valor(row[i])
        # Lista de campos que devem ser string
        campos_texto = ['id_grupo', 'id_secao', 'id_filial', 'id_cliente', 
                       'id_vendedor', 'id_transportadora', 'id_formapag', 
                       'tipo', 'id_fabricante', 'id_fornecedor'] 
        if key in campos_texto and val is not None: val = str(val)
        data[key] = val
    
    if 'id' in data: 
        data['id_original'] = str(data['id'])
        del data['id'] 
    return data

def enviar_lote(endpoint, payload):
    try:
        r = requests.post(f"{API_URL}{endpoint}", json=payload, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=120)
        return r.status_code == 200
    except: return False

def marcar_como_sincronizado(tabela_sql, ids, campo_id="ID"):
    if not ids: return
    conn = get_connection()
    if not conn: return
    cursor = conn.cursor()
    try:
        cursor.executemany(f"UPDATE {tabela_sql} SET SYNC_DASH = 'S' WHERE {campo_id} = ?", [(x,) for x in ids])
        conn.commit()
    except Exception as e: print(f"Erro Update: {e}"); conn.rollback()
    finally: conn.close()

# --- SYNC GENÉRICO ---
def sync_controle_banco(nome_tabela, endpoint, tabela_sql, colunas_sql="*", campo_id="ID", filtro_extra=""):
    print(f"[{nome_tabela}]", end=" ", flush=True)
    where = "WHERE SYNC_DASH = 'N'"
    if filtro_extra: where += f" AND {filtro_extra}"
    
    conn = get_connection()
    if not conn: return False
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT FIRST {TAMANHO_LOTE} {colunas_sql} FROM {tabela_sql} {where}")
        rows = cursor.fetchall()
        col_names = [d[0] for d in cursor.description] # Nomes das colunas retornadas pelo banco (respeita o AS alias)
    except Exception as e: print(f"Erro SQL {tabela_sql}: {e}"); conn.close(); return False
    conn.close()

    if not rows: print(".", end="", flush=True); return False

    payload = []
    ids_upd = []
    for r in rows:
        d = row_to_dict(r, col_names)
        val_id = d.get(campo_id.lower().split('.')[-1]) or d.get('id_original')
        if val_id: ids_upd.append(val_id)
        d.pop('sync_dash', None); d.pop('rdb$db_key', None)
        payload.append(d)

    print(f"Enviando {len(payload)}...", end=" ", flush=True)
    if enviar_lote(endpoint, payload):
        marcar_como_sincronizado(tabela_sql, ids_upd, campo_id)
        print("OK!", flush=True); return True
    else:
        print("Falha API.", flush=True); return False

# --- DELEÇÃO ---
def verificar_vendas_excluidas():
    conn = get_connection(); 
    if not conn: return False
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT FIRST {TAMANHO_LOTE} ID FROM SAIDA WHERE ELIMINADO = 'S' AND SYNC_DASH = 'N'")
        rows = cursor.fetchall()
        if not rows: conn.close(); return False
        
        ids = [str(r[0]) for r in rows]
        print(f"[DELECAO] {len(ids)} vendas...", end=" ", flush=True)
        enviados = []
        for i in ids:
            if enviar_lote("/api/sync/deletar-venda", {"id_original": i}): enviados.append(i)
        
        if enviados: marcar_como_sincronizado("SAIDA", enviados, "ID")
        conn.close(); return len(enviados) > 0
    except: conn.close(); return False

# --- LOOP PRINCIPAL ---
if __name__ == "__main__":
    verificar_e_configurar_banco()
    print(f"\n--- AGENTE RODANDO ---", flush=True)
    
    while True:
        try:
            encontrou = False
            if verificar_vendas_excluidas(): encontrou = True

            # --- CADASTROS (COM ALIAS 'AS NOME' PARA CORRIGIR ESTRUTURA) ---
            
            # PRODUTO (Campos extras ID_FABRICANTE e ID_FORNECEDOR inclusos)
            cols_prod = "ID, NOME, PRECO_VENDA, CUSTO_TOTAL, ID_GRUPO, ID_FABRICANTE, ID_FORNECEDOR, ATIVO, SYNC_DASH"
            if sync_controle_banco("PRODUTO", "/api/sync/cadastros/produto", "PRODUTO", cols_prod, "ID"): encontrou = True
            
            # FABRICANTE (Mapeando FABRICANTE -> NOME)
            if sync_controle_banco("FABRICANTE", "/api/sync/cadastros/fabricante", "FABRICANTE", "ID, FABRICANTE AS NOME, SYNC_DASH", "ID"): encontrou = True
            
            # CLIENTE/FORNECEDOR (PESSOA)
            if sync_controle_banco("CLIENTE", "/api/sync/cadastros/cliente", "PESSOA", "ID, NOME, CNPJ_CPF, CIDADE, ATIVO, SYNC_DASH", "ID"): encontrou = True
            
            # VENDEDOR
            if sync_controle_banco("VENDEDOR", "/api/sync/cadastros/vendedor", "VENDEDOR", "ID, NOME, COMISSAO, ATIVO, SYNC_DASH", "ID"): encontrou = True
            
            # GRUPO (Mapeando GRUPO -> NOME)
            if sync_controle_banco("GRUPO", "/api/sync/cadastros/grupo", "GRUPO", "ID, GRUPO AS NOME, ID_SECAO, SYNC_DASH", "ID"): encontrou = True
            
            # SECAO (Mapeando SECAO -> NOME)
            if sync_controle_banco("SECAO", "/api/sync/cadastros/secao", "SECAO", "ID, SECAO AS NOME, SYNC_DASH", "ID"): encontrou = True
            
            # FORMAPAG (Mapeando FORMAPAG -> NOME)
            if sync_controle_banco("FORMAPAG", "/api/sync/cadastros/formapag", "FORMAPAG", "ID, FORMAPAG AS NOME, TIPO, SYNC_DASH", "ID"): encontrou = True
            
            # --- MOVIMENTO ---
            
            # SAIDA
            if sync_controle_banco("SAIDA", "/api/sync/saida", "SAIDA", "*", "ID", filtro_extra="(ELIMINADO IS NULL OR ELIMINADO = 'N')"): encontrou = True
            
            # ITENS DA SAIDA
            q_itens = "EXISTS (SELECT 1 FROM SAIDA S WHERE S.ID = SAIDA_PRODUTO.ID_SAIDA AND (S.ELIMINADO IS NULL OR S.ELIMINADO = 'N'))"
            if sync_controle_banco("SAIDA_PRODUTO", "/api/sync/saida_produto", "SAIDA_PRODUTO", "*", "ID", filtro_extra=q_itens): encontrou = True
            
            # PAGAMENTOS DA SAIDA
            q_pag = "EXISTS (SELECT 1 FROM SAIDA S WHERE S.ID = SAIDA_FORMAPAG.ID_SAIDA AND (S.ELIMINADO IS NULL OR S.ELIMINADO = 'N'))"
            if sync_controle_banco("SAIDA_FORMAPAG", "/api/sync/saida_formapag", "SAIDA_FORMAPAG", "*", "ID", filtro_extra=q_pag): encontrou = True

            # --- DELAYS ---
            if not encontrou:
                print(f"\r[Ocioso] Aguardando {DELAY_OCIOSO}s...   ", end="", flush=True)
                time.sleep(DELAY_OCIOSO)
            else:
                print(f" [Pausa {DELAY_ENTRE_LOTES}s]", end="\n", flush=True)
                time.sleep(DELAY_ENTRE_LOTES)

        except KeyboardInterrupt: break
        except Exception as e: print(f"\nERRO LOOP: {e}"); time.sleep(10)