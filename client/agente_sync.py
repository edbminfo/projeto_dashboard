import configparser
import requests
import fdb
import sys
import time
import os
import decimal
from datetime import datetime, date

# --- CONSTANTES ---
DELAY_OCIOSO = 30       
DELAY_ENTRE_LOTES = 2   
TIMEOUT_API = 60 

# --- CARREGAMENTO CONFIG ---
print(">> Carregando Agente Sync v35.0 (Correção Inteligente)...", flush=True)
diretorio_base = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(diretorio_base, 'config.ini')

config = configparser.ConfigParser()
try:
    config.read(config_path)
    if not config.sections(): raise Exception("Arquivo config.ini vazio.")
    API_URL = config.get('API', 'url_base', fallback="https://api-dash.bmhelp.click")
    TAMANHO_LOTE = config.getint('CONFIG', 'tamanho_lote', fallback=20)
    TOKEN = config.get('API', 'token_loja', fallback=None)
    if not TOKEN: raise Exception("Token não encontrado.")
    DB_PATH = config.get('DATABASE', 'caminho')
    DB_USER = config.get('DATABASE', 'usuario')
    DB_PASS = config.get('DATABASE', 'senha')
    DB_HOST = config.get('DATABASE', 'host')
    DB_PORT = config.get('DATABASE', 'port')
except Exception as e: print(f"ERRO CONFIG: {e}"); sys.exit(1)

# --- CONEXÃO ---
def get_connection():
    try: return fdb.connect(host=DB_HOST, port=int(DB_PORT), database=DB_PATH, user=DB_USER, password=DB_PASS, charset='ISO8859_1')
    except Exception as e: print(f"   [FALHA BANCO]: {e}", flush=True); return None

# --- INSTALADOR & CORRETOR INTELIGENTE ---
def verificar_e_configurar_banco():
    conn = get_connection()
    if not conn: sys.exit(1)
    cursor = conn.cursor()

    tabelas_cadastro = ['PRODUTO', 'PESSOA', 'VENDEDOR', 'GRUPO', 'SECAO', 'FORMAPAG', 'FABRICANTE', 'FAMILIA', 'USUARIOS']
    tabelas_movimento = ['SAIDA', 'SAIDA_PRODUTO', 'SAIDA_FORMAPAG']
    todas = tabelas_cadastro + tabelas_movimento

    primeira_instalacao = False
    try:
        cursor.execute("SELECT FIRST 1 SYNK_DASH_PEND FROM PRODUTO")
        conn.commit()
    except: primeira_instalacao = True

    print("\n--- VERIFICANDO ESTRUTURA E DADOS ---")
    
    # 1. GARANTIA DE COLUNAS
    for tbl in todas:
        col_existe = False
        try:
            cursor.execute(f"SELECT FIRST 1 SYNK_DASH_PEND FROM {tbl}")
            conn.commit()
            col_existe = True
        except: pass

        if not col_existe:
            print(f">> Criando coluna em {tbl}...", flush=True)
            try:
                cursor.execute(f"ALTER TABLE {tbl} ADD SYNK_DASH_PEND CHAR(1) DEFAULT 'S'")
                conn.commit()
            except: pass
            try:
                cursor.execute(f"""
                CREATE OR ALTER TRIGGER TG_SYNK_PEND_{tbl} FOR {tbl}
                ACTIVE BEFORE INSERT OR UPDATE POSITION 0
                AS BEGIN
                    IF (INSERTING) THEN NEW.SYNK_DASH_PEND = 'S';
                    ELSE IF (NEW.SYNK_DASH_PEND = OLD.SYNK_DASH_PEND) THEN NEW.SYNK_DASH_PEND = 'S';
                END
                """)
                conn.commit()
            except: pass

    # 2. AUTO-CORREÇÃO INTELIGENTE (Só marca 'S' se valer a pena)
    print(">> Aplicando correção inteligente nos NULOS...", flush=True)
    
    # A. Cadastros: Se for nulo, marca 'S' (sempre ativo)
    for tbl in tabelas_cadastro:
        try:
            cursor.execute(f"UPDATE {tbl} SET SYNK_DASH_PEND = 'S' WHERE SYNK_DASH_PEND IS NULL")
            conn.commit()
        except: pass

    # B. Capa das Vendas (SAIDA)
    # Se cancelado (ELIMINADO='S') e NULO -> Marca 'N' (Não sobe)
    cursor.execute("UPDATE SAIDA SET SYNK_DASH_PEND = 'N' WHERE SYNK_DASH_PEND IS NULL AND ELIMINADO = 'S'")
    # O restante (Ativas) e NULO -> Marca 'S' (Sobe)
    cursor.execute("UPDATE SAIDA SET SYNK_DASH_PEND = 'S' WHERE SYNK_DASH_PEND IS NULL")
    conn.commit()

    # C. Itens e Pagamentos (Dependem da Capa)
    # Se a capa vai subir ('S'), o item NULO vira 'S'
    cursor.execute("""
        UPDATE SAIDA_PRODUTO SP SET SYNK_DASH_PEND = 'S' 
        WHERE SYNK_DASH_PEND IS NULL 
        AND EXISTS (SELECT 1 FROM SAIDA S WHERE S.ID = SP.ID_SAIDA AND S.SYNK_DASH_PEND = 'S')
    """)
    cursor.execute("""
        UPDATE SAIDA_FORMAPAG SF SET SYNK_DASH_PEND = 'S' 
        WHERE SYNK_DASH_PEND IS NULL 
        AND EXISTS (SELECT 1 FROM SAIDA S WHERE S.ID = SF.ID_SAIDA AND S.SYNK_DASH_PEND = 'S')
    """)
    
    # Se a capa NÃO vai subir (é 'N' ou velha), o item NULO vira 'N'
    cursor.execute("UPDATE SAIDA_PRODUTO SET SYNK_DASH_PEND = 'N' WHERE SYNK_DASH_PEND IS NULL")
    cursor.execute("UPDATE SAIDA_FORMAPAG SET SYNK_DASH_PEND = 'N' WHERE SYNK_DASH_PEND IS NULL")
    conn.commit()

    # 3. INSTALAÇÃO INICIAL (DATA DE CORTE)
    if primeira_instalacao:
        print("\n=== INSTALAÇÃO INICIAL ===")
        while True:
            d = input(">> Sincronizar vendas a partir de (dd/mm/aaaa): ")
            try:
                data_corte = datetime.strptime(d, "%d/%m/%Y").date()
                data_str = data_corte.strftime("%Y-%m-%d")
                break
            except: print("Data inválida.")

        print(">> Configurando data de corte...")
        # Marca tudo inicial como 'S'
        for tbl in todas:
            try: cursor.execute(f"UPDATE {tbl} SET SYNK_DASH_PEND = 'S'"); conn.commit()
            except: pass

        # Filtra velhas
        cursor.execute(f"UPDATE SAIDA SET SYNK_DASH_PEND = 'N' WHERE DATA < '{data_str}'")
        cursor.execute(f"UPDATE SAIDA SET SYNK_DASH_PEND = 'N' WHERE ELIMINADO = 'S'")
        
        # Sincroniza Itens com a Capa novamente
        cursor.execute("UPDATE SAIDA_PRODUTO SP SET SYNK_DASH_PEND = 'N' WHERE EXISTS (SELECT 1 FROM SAIDA S WHERE S.ID = SP.ID_SAIDA AND S.SYNK_DASH_PEND = 'N')")
        cursor.execute("UPDATE SAIDA_FORMAPAG SF SET SYNK_DASH_PEND = 'N' WHERE EXISTS (SELECT 1 FROM SAIDA S WHERE S.ID = SF.ID_SAIDA AND S.SYNK_DASH_PEND = 'N')")
        
        conn.commit()
        print(">> Configuração concluída!")
        time.sleep(2)
    
    conn.close()

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
        campos_texto = ['id_grupo', 'id_secao', 'id_filial', 'id_cliente', 
                       'id_vendedor', 'id_transportadora', 'id_formapag', 
                       'tipo', 'id_fabricante', 'id_fornecedor', 'id_familia', 
                       'terminal', 'id_usuario']
        if key in campos_texto and val is not None: val = str(val)
        data[key] = val
    if 'id' in data: 
        data['id_original'] = str(data['id'])
        del data['id'] 
    return data

def get_hora():
    return datetime.now().strftime("%H:%M:%S")

# --- ENVIO ROBUSTO ---
def enviar_e_validar(endpoint, payload, tabela_sql, db_keys_para_baixar, msg_log):
    try:
        print(f"\n[{get_hora()}] {msg_log}", end=" ", flush=True)
        r = requests.post(f"{API_URL}{endpoint}", json=payload, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=TIMEOUT_API)

        if r.status_code == 200:
            conn = get_connection()
            if conn:
                try:
                    cursor = conn.cursor()
                    cursor.executemany(f"UPDATE {tabela_sql} SET SYNK_DASH_PEND = 'N' WHERE RDB$DB_KEY = ?", [(x,) for x in db_keys_para_baixar])
                    conn.commit()
                    print("✅ Sucesso", flush=True)
                    conn.close()
                    return True
                except Exception as e_db:
                    print(f"❌ Erro Baixa Local: {e_db}")
                    conn.rollback(); conn.close()
                    return False
        else:
            print(f"❌ Falha API ({r.status_code})", flush=True)
            return False
    except Exception as e:
        print(f"❌ Erro Conexão: {e}", flush=True)
        return False

# --- SYNC CORE ---
def sync_controle_banco(nome_tabela, endpoint, tabela_sql, colunas_sql="*", filtro_extra=""):
    where = "WHERE SYNK_DASH_PEND = 'S'"
    if filtro_extra: where += f" AND {filtro_extra}"
    
    conn = get_connection()
    if not conn: return False
    cursor = conn.cursor()
    try:
        sql = f"SELECT FIRST {TAMANHO_LOTE} RDB$DB_KEY, {colunas_sql} FROM {tabela_sql} {where}"
        cursor.execute(sql)
        rows = cursor.fetchall()
        col_names = [d[0] for d in cursor.description][1:] 
    except Exception as e: conn.close(); return False
    conn.close()

    if not rows: 
        print(".", end="", flush=True)
        return False

    payload = []
    db_keys = []
    for r in rows:
        db_keys.append(r[0])
        d = row_to_dict(r[1:], col_names)
        d.pop('synk_dash_pend', None); d.pop('rdb$db_key', None); d.pop('sync_dash', None)
        payload.append(d)

    msg = f"[{nome_tabela}] Enviando {len(rows)} registros..."
    return enviar_e_validar(endpoint, payload, tabela_sql, db_keys, msg)

def verificar_vendas_excluidas():
    conn = get_connection(); cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT FIRST {TAMANHO_LOTE} RDB$DB_KEY, ID FROM SAIDA WHERE ELIMINADO = 'S' AND SYNK_DASH_PEND = 'S'")
        rows = cursor.fetchall()
        if not rows: conn.close(); return False
        
        db_keys = [r[0] for r in rows]
        ids_originais = [str(r[1]) for r in rows]
        
        payload = [{"id_original": i} for i in ids_originais]
        msg = f"[DELECAO] Removendo {len(ids_originais)} vendas canceladas..."
        return enviar_e_validar("/api/sync/deletar-venda", payload, "SAIDA", db_keys, msg)
    except: conn.close(); return False

# --- MAIN ---
if __name__ == "__main__":
    verificar_e_configurar_banco()
    print(f"\n--- AGENTE RODANDO (v35.0 - Final + Inteligente) ---", flush=True)
    
    while True:
        try:
            encontrou = False
            
            if verificar_vendas_excluidas(): encontrou = True

            # 1. ORDEM SOLICITADA
            if sync_controle_banco("USUARIOS", "/api/sync/cadastros/usuario_pdv", "USUARIOS", "ID, NOME, LOGIN, SYNK_DASH_PEND"): encontrou = True
            if sync_controle_banco("SECAO", "/api/sync/cadastros/secao", "SECAO", "ID, SECAO AS NOME, SYNK_DASH_PEND"): encontrou = True
            if sync_controle_banco("GRUPO", "/api/sync/cadastros/grupo", "GRUPO", "ID, GRUPO AS NOME, ID_SECAO, SYNK_DASH_PEND"): encontrou = True
            if sync_controle_banco("FAMILIA", "/api/sync/cadastros/familia", "FAMILIA", "ID, FAMILIA AS NOME, SYNK_DASH_PEND"): encontrou = True
            if sync_controle_banco("VENDEDOR", "/api/sync/cadastros/vendedor", "VENDEDOR", "ID, NOME, COMISSAO, ATIVO, SYNK_DASH_PEND"): encontrou = True
            
            # 2. PRODUTOS
            cols_prod = "ID, NOME, PRECO_VENDA, CUSTO_TOTAL, ID_GRUPO, ID_FABRICANTE, ID_FORNECEDOR, ID_FAMILIA, ATIVO, SYNK_DASH_PEND"
            if sync_controle_banco("PRODUTO", "/api/sync/cadastros/produto", "PRODUTO", cols_prod): encontrou = True
            
            # 3. COMPLEMENTOS
            if sync_controle_banco("FABRICANTE", "/api/sync/cadastros/fabricante", "FABRICANTE", "ID, FABRICANTE AS NOME, SYNK_DASH_PEND"): encontrou = True
            if sync_controle_banco("CLIENTE", "/api/sync/cadastros/cliente", "PESSOA", "ID, NOME, CNPJ_CPF, CIDADE, ATIVO, SYNK_DASH_PEND"): encontrou = True
            if sync_controle_banco("FORMAPAG", "/api/sync/cadastros/formapag", "FORMAPAG", "ID, FORMAPAG AS NOME, TIPO, SYNK_DASH_PEND"): encontrou = True
            
            # 4. MOVIMENTO
            cols_saida = "SAIDA.*, USUARIO AS ID_USUARIO"
            if sync_controle_banco("SAIDA", "/api/sync/saida", "SAIDA", cols_saida, filtro_extra="(ELIMINADO IS NULL OR ELIMINADO = 'N')"): encontrou = True
            
            # FILTRO: Itens só sobem se a capa for válida (Sem filtro pesado, pois a correção inteligente já ajustou os 'S')
            if sync_controle_banco("SAIDA_PRODUTO", "/api/sync/saida_produto", "SAIDA_PRODUTO", "*"): encontrou = True
            if sync_controle_banco("SAIDA_FORMAPAG", "/api/sync/saida_formapag", "SAIDA_FORMAPAG", "*"): encontrou = True

            if not encontrou:
                time.sleep(DELAY_OCIOSO)
                print(".", end="", flush=True)
            else:
                time.sleep(DELAY_ENTRE_LOTES)

        except KeyboardInterrupt: break
        except Exception as e: print(f"\nERRO LOOP: {e}"); time.sleep(10)