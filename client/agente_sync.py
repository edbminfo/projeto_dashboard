import configparser
import requests
import fdb
import sys
import time
import os
import decimal
from datetime import datetime, date

# --- CONFIGURAÇÕES ---
print(">> Carregando Agente Sync v21.0 (Com Deleção de Vendas)...", flush=True)
diretorio_base = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(diretorio_base, 'config.ini')

config = configparser.ConfigParser()
config.read(config_path)

try:
    API_URL = config['API']['url_base']
    TOKEN = config['API']['token_loja']
    DB_PATH = config['DATABASE']['caminho']
    DB_USER = config['DATABASE']['usuario']
    DB_PASS = config['DATABASE']['senha']
    DB_HOST = config['DATABASE']['host']
    DB_PORT = config['DATABASE']['port']
    TAMANHO_LOTE = int(config['CONFIG'].get('tamanho_lote', 50))
except Exception as e:
    print(f"ERRO DE CONFIG: {e}"); sys.exit(1)

# --- CONEXÃO BANCO ---
def get_connection():
    try:
        return fdb.connect(host=DB_HOST, port=int(DB_PORT), database=DB_PATH, user=DB_USER, password=DB_PASS, charset='ISO8859_1')
    except Exception as e:
        print(f"   [FALHA CONEXÃO BANCO]: {e}", flush=True)
        return None

# --- INSTALADOR AUTOMÁTICO ---
def verificar_e_configurar_banco():
    conn = get_connection()
    if not conn: sys.exit(1)
    cursor = conn.cursor()

    tabelas_cadastro = ['PRODUTO', 'PESSOA', 'VENDEDOR', 'GRUPO', 'SECAO']
    tabelas_movimento = ['SAIDA', 'SAIDA_PRODUTO', 'SAIDA_FORMAPAG']
    todas_tabelas = tabelas_cadastro + tabelas_movimento

    precisa_instalar = False
    try: cursor.execute("SELECT FIRST 1 SYNC_DASH FROM PRODUTO")
    except: precisa_instalar = True
    conn.commit()

    if not precisa_instalar:
        print(">> Banco já configurado.", flush=True); conn.close(); return

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
        cursor.execute(f"UPDATE {tbl} SET SYNC_DASH = 'N'")
        conn.commit()

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
        campos_texto = ['id_grupo', 'id_secao', 'id_filial', 'id_cliente', 'id_vendedor', 'id_transportadora', 'id_formapag', 'tipo']
        if key in campos_texto and val is not None: val = str(val)
        data[key] = val
    
    if 'id' in data: 
        data['id_original'] = str(data['id'])
        del data['id'] 
    return data

def enviar_lote(endpoint, payload):
    try:
        url = f"{API_URL}{endpoint}"
        r = requests.post(url, json=payload, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=120)
        return r.status_code == 200
    except Exception as e:
        print(f"   [ERRO POST]: {e}", flush=True); return False

def marcar_como_sincronizado(tabela_sql, ids_processados, campo_id="ID"):
    if not ids_processados: return
    conn = get_connection()
    if not conn: return
    cursor = conn.cursor()
    try:
        sql_update = f"UPDATE {tabela_sql} SET SYNC_DASH = 'S' WHERE {campo_id} = ?"
        params = [(x,) for x in ids_processados]
        cursor.executemany(sql_update, params)
        conn.commit()
    except Exception as e:
        print(f"   [ERRO UPDATE LOCAL]: {e}"); conn.rollback()
    finally: conn.close()

# --- SINCRONIZAÇÃO NORMAL (INSERT/UPDATE) ---
def sync_controle_banco(nome_tabela, endpoint, tabela_sql, colunas_sql="*", campo_id="ID", filtro_extra=""):
    print(f"[{nome_tabela}]", end=" ", flush=True)
    
    # MODIFICAÇÃO: Filtra apenas o que NÃO está eliminado
    where_clause = "WHERE SYNC_DASH = 'N'"
    if filtro_extra:
        where_clause += f" AND {filtro_extra}"
    
    sql = f"SELECT FIRST {TAMANHO_LOTE} {colunas_sql} FROM {tabela_sql} {where_clause}"
    
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
        print(".", end="", flush=True); return False

    payload = []
    ids_para_atualizar = []
    
    for r in rows:
        d = row_to_dict(r, col_names)
        val_id = None
        col_cursor = campo_id.lower().split('.')[-1]
        
        if col_cursor in d: val_id = d[col_cursor]
        elif 'id_original' in d: val_id = d['id_original']
             
        if val_id: ids_para_atualizar.append(val_id)
        d.pop('rdb$db_key', None); d.pop('sync_dash', None) 
        payload.append(d)

    print(f"Enviando {len(payload)}...", end=" ", flush=True)
    
    if enviar_lote(endpoint, payload):
        marcar_como_sincronizado(tabela_sql, ids_para_atualizar, campo_id)
        print("OK!", flush=True)
        return True
    else:
        print("Falha API.", flush=True)
        return False

# --- FUNÇÃO NOVA: PROCESSAR DELEÇÕES ---
def verificar_vendas_excluidas():
    """
    Busca vendas marcadas como ELIMINADO='S' mas ainda pendentes de sync (SYNC_DASH='N')
    Envia comando de deleção para o servidor.
    """
    conn = get_connection()
    if not conn: return False
    cursor = conn.cursor()
    
    try:
        # Busca ID das vendas eliminadas pendentes de aviso
        cursor.execute(f"SELECT FIRST {TAMANHO_LOTE} ID FROM SAIDA WHERE ELIMINADO = 'S' AND SYNC_DASH = 'N'")
        rows = cursor.fetchall()
        
        if not rows: 
            conn.close()
            return False

        print(f"[DELECAO] Encontradas {len(rows)} vendas eliminadas...", end=" ", flush=True)
        
        ids_sucesso = []
        for r in rows:
            id_venda = str(r[0])
            payload = {"id_original": id_venda}
            
            # Chama a rota específica de deleção
            if enviar_lote("/api/sync/deletar-venda", payload):
                ids_sucesso.append(id_venda)
        
        conn.close() # Fecha leitura para abrir update
        
        # Marca como Sincronizado ('S') para não tentar apagar de novo
        if ids_sucesso:
            marcar_como_sincronizado("SAIDA", ids_sucesso, "ID")
            print(f"Processadas: {len(ids_sucesso)} OK!")
            return True
        else:
            print("Falha ao enviar.")
            return False

    except Exception as e:
        print(f"Erro Check Deleção: {e}"); conn.close(); return False

# --- LOOP PRINCIPAL ---
if __name__ == "__main__":
    verificar_e_configurar_banco()
    print(f"\n--- AGENTE EM EXECUÇÃO ---", flush=True)
    
    while True:
        try:
            dados_encontrados = False
            
            # 1. Verifica se tem coisa para deletar primeiro
            if verificar_vendas_excluidas(): dados_encontrados = True

            # 2. Cadastros (Normal)
            if sync_controle_banco("PRODUTO", "/api/sync/cadastros/produto", "PRODUTO", "ID, NOME, PRECO_VENDA, CUSTO_TOTAL, ID_GRUPO, ATIVO, SYNC_DASH", "ID"): dados_encontrados = True
            if sync_controle_banco("CLIENTE", "/api/sync/cadastros/cliente", "PESSOA", "ID, NOME, CNPJ_CPF, CIDADE, ATIVO, SYNC_DASH", "ID"): dados_encontrados = True
            if sync_controle_banco("VENDEDOR", "/api/sync/cadastros/vendedor", "VENDEDOR", "ID, NOME, COMISSAO, ATIVO, SYNC_DASH", "ID"): dados_encontrados = True
            if sync_controle_banco("GRUPO", "/api/sync/cadastros/grupo", "GRUPO", "ID, GRUPO, ID_SECAO, SYNC_DASH", "ID"): dados_encontrados = True
            if sync_controle_banco("SECAO", "/api/sync/cadastros/secao", "SECAO", "ID, SECAO, SYNC_DASH", "ID"): dados_encontrados = True
            
            # 3. Movimento (Vendas)
            # AQUI: Adicionado filtro_extra para ignorar ELIMINADO='S' na subida normal
            # Se for 'S', a função verificar_vendas_excluidas cuidará dele.
            if sync_controle_banco("SAIDA", "/api/sync/saida", "SAIDA", "*", "ID", filtro_extra="(ELIMINADO IS NULL OR ELIMINADO = 'N')"): dados_encontrados = True
            
            # Itens e Pagamentos não têm campo ELIMINADO, então filtramos pelo pai (SAIDA)
            # Se a SAIDA for 'S', não enviamos os itens (eles serão deletados em cascata pelo ID da venda)
            sql_filtro_itens = """
                EXISTS (SELECT 1 FROM SAIDA S 
                        WHERE S.ID = SAIDA_PRODUTO.ID_SAIDA 
                        AND (S.ELIMINADO IS NULL OR S.ELIMINADO = 'N'))
            """
            if sync_controle_banco("SAIDA_PRODUTO", "/api/sync/saida_produto", "SAIDA_PRODUTO", "*", "ID", filtro_extra=sql_filtro_itens): dados_encontrados = True
            
            sql_filtro_pag = """
                EXISTS (SELECT 1 FROM SAIDA S 
                        WHERE S.ID = SAIDA_FORMAPAG.ID_SAIDA 
                        AND (S.ELIMINADO IS NULL OR S.ELIMINADO = 'N'))
            """
            if sync_controle_banco("SAIDA_FORMAPAG", "/api/sync/saida_formapag", "SAIDA_FORMAPAG", "*", "ID", filtro_extra=sql_filtro_pag): dados_encontrados = True

            if not dados_encontrados:
                print(".", end="", flush=True)
                time.sleep(5) 
            else:
                time.sleep(0.1) 
            
        except KeyboardInterrupt:
            print("\nParando...", flush=True); break
        except Exception as e:
            print(f"\n[ERRO LOOP]: {e}", flush=True); time.sleep(10)