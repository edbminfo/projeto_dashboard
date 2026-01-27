from fastapi import APIRouter, Depends, HTTPException
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from security import validar_token
from database_utils import get_db_connection

router = APIRouter()

# --- HELPER DINÂMICO ---
def inserir_dados_dinamicos(cursor, schema_banco, tabela, dados, upsert=True):
    if not dados: return
    
    # 1. ORDENAÇÃO ANTI-DEADLOCK
    # Garante que as linhas sejam sempre acessadas na ordem do ID.
    if upsert and 'id_original' in dados[0]:
        dados.sort(key=lambda x: str(x.get('id_original', '')))
    elif 'id_saida' in dados[0]:
        dados.sort(key=lambda x: str(x.get('id_saida', '')))

    # 2. SCHEMA EVOLUTION
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_banco}' AND table_name = '{tabela}'")
    colunas_banco = {row[0] for row in cursor.fetchall()}
    
    novas_colunas = []
    amostra = dados[0]
    
    for coluna, valor in amostra.items():
        if coluna not in colunas_banco:
            tipo_sql = "TEXT"
            if isinstance(valor, int): tipo_sql = "BIGINT"
            elif isinstance(valor, float): tipo_sql = "DECIMAL(18,4)"
            novas_colunas.append(f"ADD COLUMN IF NOT EXISTS \"{coluna}\" {tipo_sql}")
            colunas_banco.add(coluna)
            
    if novas_colunas:
        try:
            cursor.execute(f"ALTER TABLE {schema_banco}.{tabela} {', '.join(novas_colunas)};")
        except Exception as e:
            print(f"Erro Schema {tabela}: {e}")

    # 3. INSERÇÃO
    for linha in dados:
        cols = list(linha.keys())
        vals = list(linha.values())
        
        cols_str = ', '.join([f'"{c}"' for c in cols])
        placeholders = ', '.join(['%s'] * len(vals))
        
        if upsert and 'id_original' in cols:
            campos_ignorar = ('id_original', 'uuid_id', 'id_saida', 'id_produto', 'id_formapag')
            update_parts = [f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in campos_ignorar]
            
            if update_parts:
                sql = f"INSERT INTO {schema_banco}.{tabela} ({cols_str}) VALUES ({placeholders}) ON CONFLICT (id_original) DO UPDATE SET {', '.join(update_parts)}"
            else:
                sql = f"INSERT INTO {schema_banco}.{tabela} ({cols_str}) VALUES ({placeholders}) ON CONFLICT (id_original) DO NOTHING"
        else:
            sql = f"INSERT INTO {schema_banco}.{tabela} ({cols_str}) VALUES ({placeholders})"
            
        cursor.execute(sql, vals)

# --- ROTAS DE TABELAS FATO ---

@router.post("/sync/saida")
def receber_saida(dados: List[Dict[str, Any]], schema: str = Depends(validar_token)):
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        inserir_dados_dinamicos(cursor, schema, "saida", dados)
        conn.commit()
        return {"status": "ok"}
    except Exception as e:
        conn.rollback(); print(f"Erro Saida: {e}"); raise HTTPException(500, str(e))
    finally: conn.close()

@router.post("/sync/saida_produto")
def receber_saida_produto(dados: List[Dict[str, Any]], schema: str = Depends(validar_token)):
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        inserir_dados_dinamicos(cursor, schema, "saida_produto", dados)
        conn.commit()
        return {"status": "ok"}
    except Exception as e:
        conn.rollback(); print(f"Erro SaidaProduto: {e}"); raise HTTPException(500, str(e))
    finally: conn.close()

@router.post("/sync/saida_formapag")
def receber_saida_formapag(dados: List[Dict[str, Any]], schema: str = Depends(validar_token)):
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        # CORREÇÃO CRÍTICA DO ERRO 500:
        # Extraímos os IDs e garantimos que são STRING (str) antes de enviar para o banco
        ids_saida = sorted(list({str(d['id_saida']) for d in dados if 'id_saida' in d}))
        
        if ids_saida:
            # Usamos placeholders %s para o driver tratar a string corretamente
            placeholders = ','.join(['%s'] * len(ids_saida))
            sql_delete = f"DELETE FROM {schema}.saida_formapag WHERE id_saida IN ({placeholders})"
            cursor.execute(sql_delete, ids_saida)
            
        # Insere os novos (upsert=False pois não tem PK única simples)
        inserir_dados_dinamicos(cursor, schema, "saida_formapag", dados, upsert=False)
        conn.commit()
        return {"status": "ok"}
    except Exception as e:
        conn.rollback(); print(f"Erro SaidaFormapag: {e}"); raise HTTPException(500, str(e))
    finally: conn.close()

# --- ROTAS DE CADASTROS (COMPLETO) ---

@router.post("/sync/cadastros/{tipo}")
def receber_cadastros(tipo: str, dados: List[Dict[str, Any]], schema: str = Depends(validar_token)):
    conn = get_db_connection(); cursor = conn.cursor()
    tabela_map = {
        "cliente": "clientes", "vendedor": "vendedores", 
        "secao": "secoes", "grupo": "grupos", 
        "produto": "produtos", "formapag": "formapag"
    }
    
    if tipo not in tabela_map: raise HTTPException(400, "Tipo inválido")
    tabela = f"{schema}.{tabela_map[tipo]}"
    
    # Ordenação
    if dados and 'id_original' in dados[0]:
        dados.sort(key=lambda x: str(x.get('id_original', '')))
    
    try:
        if tipo == "cliente":
            sql = f"INSERT INTO {tabela} (id_original, nome, cpf_cnpj, cidade, ativo) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id_original) DO UPDATE SET nome=EXCLUDED.nome, cpf_cnpj=EXCLUDED.cpf_cnpj, cidade=EXCLUDED.cidade, ativo=EXCLUDED.ativo"
            for d in dados: cursor.execute(sql, (d.get('id_original'), d.get('nome'), d.get('cpf_cnpj'), d.get('cidade'), d.get('ativo')))
        
        elif tipo == "produto":
            sql = f"INSERT INTO {tabela} (id_original, nome, preco_venda, custo_total, id_grupo, ativo) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id_original) DO UPDATE SET nome=EXCLUDED.nome, preco_venda=EXCLUDED.preco_venda, custo_total=EXCLUDED.custo_total, id_grupo=EXCLUDED.id_grupo, ativo=EXCLUDED.ativo"
            for d in dados: cursor.execute(sql, (d.get('id_original'), d.get('nome'), d.get('preco_venda'), d.get('custo_total'), str(d.get('id_grupo')), d.get('ativo')))
        
        elif tipo == "formapag":
            sql = f"INSERT INTO {tabela} (id_original, nome, tipo) VALUES (%s, %s, %s) ON CONFLICT (id_original) DO UPDATE SET nome=EXCLUDED.nome, tipo=EXCLUDED.tipo"
            for d in dados: cursor.execute(sql, (d.get('id_original'), d.get('nome'), str(d.get('tipo'))))

        elif tipo == "vendedor":
            sql = f"INSERT INTO {tabela} (id_original, nome, comissao, ativo) VALUES (%s, %s, %s, %s) ON CONFLICT (id_original) DO UPDATE SET nome=EXCLUDED.nome, comissao=EXCLUDED.comissao"
            for d in dados: cursor.execute(sql, (d.get('id_original'), d.get('nome'), d.get('comissao'), d.get('ativo')))
        
        elif tipo == "secao":
            sql = f"INSERT INTO {tabela} (id_original, nome) VALUES (%s, %s) ON CONFLICT (id_original) DO UPDATE SET nome=EXCLUDED.nome"
            for d in dados: cursor.execute(sql, (d.get('id_original'), d.get('nome')))
        
        elif tipo == "grupo":
            sql = f"INSERT INTO {tabela} (id_original, nome, id_secao) VALUES (%s, %s, %s) ON CONFLICT (id_original) DO UPDATE SET nome=EXCLUDED.nome, id_secao=EXCLUDED.id_secao"
            for d in dados: cursor.execute(sql, (d.get('id_original'), d.get('nome'), str(d.get('id_secao'))))

        conn.commit()
        return {"status": "ok"}
    except Exception as e:
        conn.rollback(); raise HTTPException(500, str(e))
    finally: conn.close()