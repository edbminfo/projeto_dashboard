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

# --- CARREGAMENTO CONFIG ---
print(">> Carregando Agente Sync v27.0 (Com Data de Corte)...", flush=True)
diretorio_base = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(diretorio_base, 'config.ini')

config = configparser.ConfigParser()
try:
    config.read(config_path)
    if not config.sections(): raise Exception("Arquivo config.ini vazio.")
    
    API_URL = config.get('API', 'url_base', fallback="http://127.0.0.1:8000")
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

# --- INSTALADOR ---
def verificar_e_configurar_banco():
    conn = get_connection()
    if not conn: sys.exit(1)
    cursor = conn.cursor()

    tabelas_cadastro = ['PRODUTO', 'PESSOA', 'VENDEDOR', 'GRUPO', 'SECAO', 'FORMAPAG', 'FABRICANTE', 'FAMILIA', 'USUARIOS']
    tabelas_movimento = ['SAIDA', 'SAIDA_PRODUTO', 'SAIDA_FORMAPAG']
    todas = tabelas_cadastro + tabelas_movimento

    # 1. Verifica se é a primeira vez (baseado na tabela PRODUTO)
    primeira_instalacao = False
    try:
        cursor.execute("SELECT FIRST 1 SYNK_DASH_PEND FROM PRODUTO")
        conn.commit()
    except:
        primeira_instalacao = True

    # 2. Garante estrutura em TODAS as tabelas
    print("\n--- VERIFICANDO ESTRUTURA ---")
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
                # Default 'S' = Tudo nasce pendente, depois filtramos
                cursor.execute(f"ALTER TABLE {tbl} ADD SYNK_DASH_PEND CHAR(1) DEFAULT 'S'")
                conn.commit()
            except Exception as e: 
                if "exists" not in str(e): print(f"Erro Alter {tbl}: {e}")

            # Trigger de Monitoramento
            try:
                cursor.execute(f"""
                CREATE OR ALTER TRIGGER TG_SYNK_PEND_{tbl} FOR {tbl}
                ACTIVE BEFORE INSERT OR UPDATE POSITION 0
                AS BEGIN
                    IF (INSERTING) THEN 
                        NEW.SYNK_DASH_PEND = 'S';
                    ELSE
                        IF (NEW.SYNK_DASH_PEND = OLD.SYNK_DASH_PEND) THEN 
                            NEW.SYNK_DASH_PEND = 'S';
                END
                """)
                conn.commit()
            except Exception as e: print(f"Erro Trigger {tbl}: {e}")

    # 3. Se for instalação nova, pergunta a data e configura os dados
    if primeira_instalacao:
        print("\n" + "="*50)
        print(" INSTALAÇÃO INICIAL DETECTADA")
        print("="*50)
        
        while True:
            d = input(">> Sincronizar vendas a partir de (dd/mm/aaaa): ")
            try:
                data_corte = datetime.strptime(d, "%d/%m/%Y").date()
                data_str = data_corte.strftime("%Y-%m-%d")
                break
            except: print("Data inválida. Tente novamente.")

        print(">> Configurando registros iniciais (Aguarde)...")
        
        # A. Cadastros: Sobe tudo ('S')
        for tbl in tabelas_cadastro:
            try: cursor.execute(f"UPDATE {tbl} SET SYNK_DASH_PEND = 'S'"); conn.commit()
            except: pass

        # B. Movimento: Marca tudo como 'S' primeiro
        cursor.execute("UPDATE SAIDA SET SYNK_DASH_PEND = 'S'")
        conn.commit()

        # C. Remove da fila o que for VELHO (< data_corte)
        print(f"   - Ignorando vendas anteriores a {d}...")
        cursor.execute(f"UPDATE SAIDA SET SYNK_DASH_PEND = 'N' WHERE DATA < '{data_str}'")
        
        # D. Remove da fila o que já está CANCELADO ('S') no legado
        print("   - Ignorando vendas antigas canceladas...")
        cursor.execute(f"UPDATE SAIDA SET SYNK_DASH_PEND = 'N' WHERE ELIMINADO = 'S'")
        conn.commit()

        # E. Ajusta Itens e Pagamentos baseados na SAIDA
        print("   - Ajustando itens e pagamentos...")
        cursor.execute(f"""
            UPDATE SAIDA_PRODUTO SP SET SYNK_DASH_PEND = 'N' 
            WHERE EXISTS (SELECT 1 FROM SAIDA S WHERE S.ID = SP.ID_SAIDA AND S.SYNK_DASH_PEND = 'N')
        """)
        cursor.execute(f"""
            UPDATE SAIDA_FORMAPAG SF SET SYNK_DASH_PEND = 'N' 
            WHERE EXISTS (SELECT 1 FROM SAIDA S WHERE S.ID = SF.ID_SAIDA AND S.SYNK_DASH_PEND = 'N')
        """)
        cursor.execute(f"""
            UPDATE SAIDA_PRODUTO SP SET SYNK_DASH_PEND = 'S' 
            WHERE EXISTS (SELECT 1 FROM SAIDA S WHERE S.ID = SP.ID_SAIDA AND S.SYNK_DASH_PEND = 'S')
        """)
        cursor.execute(f"""
            UPDATE SAIDA_FORMAPAG SF SET SYNK_DASH_PEND = 'S' 
            WHERE EXISTS (SELECT 1 FROM SAIDA S WHERE S.ID = SF.ID_SAIDA AND S.SYNK_DASH_PEND = 'S')
        """)
        
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

def enviar_lote(endpoint, payload):
    try:
        r = requests.post(f"{API_URL}{endpoint}", json=payload, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=120)
        return r.status_code == 200
    except: return False

def marcar_como_sincronizado(tabela_sql, ids, campo_id="ID"):
    if not ids: return
    conn = get_connection(); cursor = conn.cursor()
    try:
        cursor.executemany(f"UPDATE {tabela_sql} SET SYNK_DASH_PEND = 'N' WHERE {campo_id} = ?", [(x,) for x in ids])
        conn.commit()
    except: conn.rollback()
    finally: conn.close()

# --- SYNC CORE ---
def sync_controle_banco(nome_tabela, endpoint, tabela_sql, colunas_sql="*", campo_id="ID", filtro_extra=""):
    print(f"[{nome_tabela}]", end=" ", flush=True)
    
    where = "WHERE SYNK_DASH_PEND = 'S'"
    if filtro_extra: where += f" AND {filtro_extra}"
    
    conn = get_connection()
    if not conn: return False
    cursor = conn.cursor()
    try:
        # Tenta pegar lotes
        cursor.execute(f"SELECT FIRST {TAMANHO_LOTE} {colunas_sql} FROM {tabela_sql} {where}")
        rows = cursor.fetchall()
        col_names = [d[0] for d in cursor.description]
    except Exception as e: 
        # print(f"Erro SQL {tabela_sql}: {e}") # Debug se precisar
        conn.close(); return False
    conn.close()

    if not rows: 
        print(".", end="", flush=True)
        return False

    # Log visual de progresso
    print(f" {len(rows)} >>", end=" ", flush=True)

    payload = []
    ids_upd = []
    for r in rows:
        d = row_to_dict(r, col_names)
        val_id = d.get(campo_id.lower().split('.')[-1]) or d.get('id_original')
        if val_id: ids_upd.append(val_id)
        d.pop('synk_dash_pend', None); d.pop('rdb$db_key', None); d.pop('sync_dash', None)
        payload.append(d)

    if enviar_lote(endpoint, payload):
        marcar_como_sincronizado(tabela_sql, ids_upd, campo_id)
        print("OK", flush=True); return True
    else: 
        print("Falha", flush=True); return False

def verificar_vendas_excluidas():
    conn = get_connection(); cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT FIRST {TAMANHO_LOTE} ID FROM SAIDA WHERE ELIMINADO = 'S' AND SYNK_DASH_PEND = 'S'")
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

# --- MAIN ---
if __name__ == "__main__":
    verificar_e_configurar_banco()
    print(f"\n--- AGENTE RODANDO (Ordem Ajustada) ---", flush=True)
    
    while True:
        try:
            encontrou = False
            
            if verificar_vendas_excluidas(): encontrou = True

            # 1. USUARIO / SECAO / GRUPO / FAMILIA / VENDEDOR
            if sync_controle_banco("USUARIOS", "/api/sync/cadastros/usuario_pdv", "USUARIOS", "ID, NOME, LOGIN, SYNK_DASH_PEND", "ID"): encontrou = True
            if sync_controle_banco("SECAO", "/api/sync/cadastros/secao", "SECAO", "ID, SECAO AS NOME, SYNK_DASH_PEND", "ID"): encontrou = True
            if sync_controle_banco("GRUPO", "/api/sync/cadastros/grupo", "GRUPO", "ID, GRUPO AS NOME, ID_SECAO, SYNK_DASH_PEND", "ID"): encontrou = True
            if sync_controle_banco("FAMILIA", "/api/sync/cadastros/familia", "FAMILIA", "ID, FAMILIA AS NOME, SYNK_DASH_PEND", "ID"): encontrou = True
            if sync_controle_banco("VENDEDOR", "/api/sync/cadastros/vendedor", "VENDEDOR", "ID, NOME, COMISSAO, ATIVO, SYNK_DASH_PEND", "ID"): encontrou = True
            
            # 2. PRODUTOS (Prioridade Alta)
            cols_prod = "ID, NOME, PRECO_VENDA, CUSTO_TOTAL, ID_GRUPO, ID_FABRICANTE, ID_FORNECEDOR, ID_FAMILIA, ATIVO, SYNK_DASH_PEND"
            if sync_controle_banco("PRODUTO", "/api/sync/cadastros/produto", "PRODUTO", cols_prod, "ID"): encontrou = True
            
            # 3. DEMAIS CADASTROS
            if sync_controle_banco("FABRICANTE", "/api/sync/cadastros/fabricante", "FABRICANTE", "ID, FABRICANTE AS NOME, SYNK_DASH_PEND", "ID"): encontrou = True
            if sync_controle_banco("CLIENTE", "/api/sync/cadastros/cliente", "PESSOA", "ID, NOME, CNPJ_CPF, CIDADE, ATIVO, SYNK_DASH_PEND", "ID"): encontrou = True
            if sync_controle_banco("FORMAPAG", "/api/sync/cadastros/formapag", "FORMAPAG", "ID, FORMAPAG AS NOME, TIPO, SYNK_DASH_PEND", "ID"): encontrou = True
            
            # 4. MOVIMENTO
            cols_saida = "SAIDA.*, USUARIO AS ID_USUARIO"
            if sync_controle_banco("SAIDA", "/api/sync/saida", "SAIDA", cols_saida, "ID", filtro_extra="(ELIMINADO IS NULL OR ELIMINADO = 'N')"): encontrou = True
            
            q_itens = "EXISTS (SELECT 1 FROM SAIDA S WHERE S.ID = SAIDA_PRODUTO.ID_SAIDA AND (S.ELIMINADO IS NULL OR S.ELIMINADO = 'N'))"
            if sync_controle_banco("SAIDA_PRODUTO", "/api/sync/saida_produto", "SAIDA_PRODUTO", "*", "ID", filtro_extra=q_itens): encontrou = True
            
            q_pag = "EXISTS (SELECT 1 FROM SAIDA S WHERE S.ID = SAIDA_FORMAPAG.ID_SAIDA AND (S.ELIMINADO IS NULL OR S.ELIMINADO = 'N'))"
            if sync_controle_banco("SAIDA_FORMAPAG", "/api/sync/saida_formapag", "SAIDA_FORMAPAG", "*", "ID", filtro_extra=q_pag): encontrou = True

            if not encontrou:
                print(f"\r[Ocioso] Aguardando {DELAY_OCIOSO}s...   ", end="", flush=True)
                time.sleep(DELAY_OCIOSO)
            else:
                print(f" [Pausa {DELAY_ENTRE_LOTES}s]", end="\n", flush=True)
                time.sleep(DELAY_ENTRE_LOTES)

        except KeyboardInterrupt: break
        except Exception as e: print(f"\nERRO LOOP: {e}"); time.sleep(10)