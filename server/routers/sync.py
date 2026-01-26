from fastapi import APIRouter, Depends, HTTPException
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from security import validar_token
from database_utils import get_db_connection

router = APIRouter()

# --- HELPER: CRIAÇÃO DINÂMICA DE COLUNAS ---
def verificar_e_criar_colunas(cursor, schema, tabela, dados_exemplo: Dict[str, Any]):
    """
    Verifica quais colunas do dict não existem no banco e executa ALTER TABLE.
    """
    # 1. Lista colunas atuais do banco
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{tabela}'")
    colunas_banco = {row[0] for row in cursor.fetchall()}
    
    # 2. Identifica colunas novas
    novas_colunas = []
    for coluna, valor in dados_exemplo.items():
        if coluna not in colunas_banco:
            # Inferência básica de tipo (Default seguro: TEXT)
            tipo_sql = "TEXT"
            if isinstance(valor, int): tipo_sql = "BIGINT"
            elif isinstance(valor, float): tipo_sql = "DECIMAL(18,4)"
            
            # Adiciona aspas na coluna para suportar nomes estranhos ou palavras reservadas
            novas_colunas.append(f"ADD COLUMN IF NOT EXISTS \"{coluna}\" {tipo_sql}")
            colunas_banco.add(coluna) # Evita duplicar na mesma passagem
            
    # 3. Aplica alteração
    if novas_colunas:
        sql = f"ALTER TABLE {schema}.{tabela} {', '.join(novas_colunas)};"
        try:
            cursor.execute(sql)
            print(f"🔧 [DYNAMIC SCHEMA] Tabela {tabela} atualizada: +{len(novas_colunas)} colunas.")
        except Exception as e:
            print(f"Erro ao criar colunas: {e}")
            raise e

# --- ROTA VENDAS (Totalmente Dinâmica) ---
@router.post("/sync/vendas")
def receber_vendas_dinamicas(dados: List[Dict[str, Any]], schema_banco: str = Depends(validar_token)):
    if not dados: return {"status": "vazio"}

    conn = get_db_connection()
    cursor = conn.cursor()
    tabela = "vendas"
    
    try:
        # 1. Ajusta o banco baseado na estrutura do primeiro registro do lote
        verificar_e_criar_colunas(cursor, schema_banco, tabela, dados[0])
        
        # 2. Insere os dados
        for linha in dados:
            colunas = list(linha.keys())
            valores = list(linha.values())
            
            # Monta Query Dinâmica Segura
            cols_str = ', '.join([f'"{c}"' for c in colunas])
            placeholders = ', '.join(['%s'] * len(valores))
            
            # Update dinâmico para UPSERT (exceto id_original e uuid_id)
            update_parts = [f'"{c}" = EXCLUDED."{c}"' for c in colunas if c not in ('id_original', 'uuid_id')]
            update_str = ', '.join(update_parts)
            
            sql = f"""
            INSERT INTO {schema_banco}.{tabela} ({cols_str})
            VALUES ({placeholders})
            ON CONFLICT (id_original) DO UPDATE SET
            {update_str}
            """
            
            cursor.execute(sql, valores)
        
        conn.commit()
        return {"status": "ok", "recebidos": len(dados)}
    
    except Exception as e:
        conn.rollback()
        print(f"Erro Sync Dinâmico: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# --- ROTA CADASTROS (Estruturada/Fixa) ---
# Mantemos os cadastros fixos para garantir integridade do Dashboard
class CadastroItem(BaseModel):
    id_original: str
    nome: str
    cpf_cnpj: Optional[str] = None
    cidade: Optional[str] = None
    ativo: Optional[str] = None
    comissao: Optional[float] = None
    id_secao: Optional[str] = None
    preco_venda: Optional[float] = None
    custo_total: Optional[float] = None
    id_grupo: Optional[str] = None

@router.post("/sync/cadastros/{tipo}")
def receber_cadastros(tipo: str, dados: List[CadastroItem], schema_banco: str = Depends(validar_token)):
    conn = get_db_connection()
    cursor = conn.cursor()
    tabela_map = {"cliente": "clientes", "vendedor": "vendedores", "secao": "secoes", "grupo": "grupos", "produto": "produtos"}
    
    if tipo not in tabela_map: raise HTTPException(400, "Tipo inválido")
    tabela = f"{schema_banco}.{tabela_map[tipo]}"
    
    try:
        if tipo == "cliente":
            sql = f"INSERT INTO {tabela} (id_original, nome, cpf_cnpj, cidade, ativo) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id_original) DO UPDATE SET nome=EXCLUDED.nome, cpf_cnpj=EXCLUDED.cpf_cnpj, cidade=EXCLUDED.cidade, ativo=EXCLUDED.ativo"
            for d in dados: cursor.execute(sql, (d.id_original, d.nome, d.cpf_cnpj, d.cidade, d.ativo))
        elif tipo == "vendedor":
            sql = f"INSERT INTO {tabela} (id_original, nome, comissao, ativo) VALUES (%s, %s, %s, %s) ON CONFLICT (id_original) DO UPDATE SET nome=EXCLUDED.nome, comissao=EXCLUDED.comissao, ativo=EXCLUDED.ativo"
            for d in dados: cursor.execute(sql, (d.id_original, d.nome, d.comissao, d.ativo))
        elif tipo == "secao":
            sql = f"INSERT INTO {tabela} (id_original, nome) VALUES (%s, %s) ON CONFLICT (id_original) DO UPDATE SET nome=EXCLUDED.nome"
            for d in dados: cursor.execute(sql, (d.id_original, d.nome))
        elif tipo == "grupo":
            sql = f"INSERT INTO {tabela} (id_original, nome, id_secao) VALUES (%s, %s, %s) ON CONFLICT (id_original) DO UPDATE SET nome=EXCLUDED.nome, id_secao=EXCLUDED.id_secao"
            for d in dados: cursor.execute(sql, (d.id_original, d.nome, d.id_secao))
        elif tipo == "produto":
            sql = f"INSERT INTO {tabela} (id_original, nome, preco_venda, custo_total, id_grupo, ativo) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id_original) DO UPDATE SET nome=EXCLUDED.nome, preco_venda=EXCLUDED.preco_venda, custo_total=EXCLUDED.custo_total, id_grupo=EXCLUDED.id_grupo, ativo=EXCLUDED.ativo"
            for d in dados: cursor.execute(sql, (d.id_original, d.nome, d.preco_venda, d.custo_total, d.id_grupo, d.ativo))
            
        conn.commit()
        return {"status": "ok"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        conn.close()