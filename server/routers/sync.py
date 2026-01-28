from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from typing import List, Optional
from security import validar_token
from database_utils import get_db_connection

router = APIRouter()

# --- MODELO PARA DELEÇÃO ---
class DeleteVendaSchema(BaseModel):
    id_original: str

# --- UTILS ---
def get_existing_columns(cursor, schema, tabela):
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{tabela}'")
    return {row[0] for row in cursor.fetchall()}

# --- UPSERT INTELIGENTE ---
def upsert_generico(schema: str, tabela: str, dados: List[dict]):
    if not dados: return {"status": "vazio"}
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        chaves_json = [k for k in dados[0].keys() if k not in ('id_original', 'id')]
        cols_create = ", ".join([f"{k} TEXT" for k in chaves_json])
        
        sql_create = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{tabela} (
                uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                id_original VARCHAR(50),
                criado_em TIMESTAMP DEFAULT NOW(),
                modificado_em TIMESTAMP DEFAULT NOW(),
                {cols_create}
            )
        """
        cursor.execute(sql_create)
        
        colunas_banco = get_existing_columns(cursor, schema, tabela)
        for col in chaves_json:
            if col not in colunas_banco:
                cursor.execute(f"ALTER TABLE {schema}.{tabela} ADD COLUMN {col} TEXT")

        if 'modificado_em' not in colunas_banco: cursor.execute(f"ALTER TABLE {schema}.{tabela} ADD COLUMN modificado_em TIMESTAMP DEFAULT NOW()")
        if 'id_original' not in colunas_banco: cursor.execute(f"ALTER TABLE {schema}.{tabela} ADD COLUMN id_original VARCHAR(50)")

        try:
            cursor.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{tabela}_id_original ON {schema}.{tabela} (id_original)")
        except: pass

        for item in dados:
            campos = [k for k in item.keys() if k != 'id'] 
            valores = [str(item[k]) if item[k] is not None else None for k in campos]
            placeholders = ", ".join(["%s"] * len(valores))
            cols_insert = ", ".join(campos)
            
            update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in campos if c != 'id_original'])
            if update_set: update_set += ", modificado_em = NOW()"
            else: update_set = "modificado_em = NOW()"
            
            sql_insert = f"""
                INSERT INTO {schema}.{tabela} ({cols_insert})
                VALUES ({placeholders})
                ON CONFLICT (id_original) DO UPDATE SET {update_set}
            """
            cursor.execute(sql_insert, valores)
            
        conn.commit()
        return {"status": "sucesso", "tabela": tabela, "qtd": len(dados)}
    except Exception as e:
        conn.rollback()
        print(f"Erro Sync {tabela}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally: conn.close()

# --- ROTA DE DELEÇÃO ---
@router.post("/sync/deletar-venda")
def deletar_venda(dados: DeleteVendaSchema, schema: str = Depends(validar_token)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"DELETE FROM {schema}.saida_produto WHERE id_saida = %s", (dados.id_original,))
        cursor.execute(f"DELETE FROM {schema}.saida_formapag WHERE id_saida = %s", (dados.id_original,))
        cursor.execute(f"DELETE FROM {schema}.saida WHERE id_original = %s", (dados.id_original,))
        conn.commit()
        return {"status": "deletado", "id": dados.id_original}
    except Exception as e:
        conn.rollback()
        if "undefined table" in str(e): return {"status": "ignorado"}
        print(f"Erro ao deletar venda {dados.id_original}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally: conn.close()

# --- ROTAS DE CADASTRO ---
@router.post("/sync/cadastros/produto")
async def sync_produto(request: Request, schema: str = Depends(validar_token)): 
    return upsert_generico(schema, "produto", await request.json())

@router.post("/sync/cadastros/cliente")
async def sync_cliente(request: Request, schema: str = Depends(validar_token)): 
    return upsert_generico(schema, "cliente", await request.json())

@router.post("/sync/cadastros/vendedor")
async def sync_vendedor(request: Request, schema: str = Depends(validar_token)): 
    return upsert_generico(schema, "vendedor", await request.json())

@router.post("/sync/cadastros/grupo")
async def sync_grupo(request: Request, schema: str = Depends(validar_token)): 
    return upsert_generico(schema, "grupo", await request.json())

@router.post("/sync/cadastros/secao")
async def sync_secao(request: Request, schema: str = Depends(validar_token)): 
    return upsert_generico(schema, "secao", await request.json())

@router.post("/sync/cadastros/formapag")
async def sync_formapag(request: Request, schema: str = Depends(validar_token)): 
    return upsert_generico(schema, "formapag", await request.json())

@router.post("/sync/cadastros/fabricante")
async def sync_fabricante(request: Request, schema: str = Depends(validar_token)): 
    return upsert_generico(schema, "fabricante", await request.json())

@router.post("/sync/cadastros/familia")
async def sync_familia(request: Request, schema: str = Depends(validar_token)): 
    return upsert_generico(schema, "familia", await request.json())

# --- NOVA ROTA: USUÁRIO PDV ---
@router.post("/sync/cadastros/usuario_pdv")
async def sync_usuario_pdv(request: Request, schema: str = Depends(validar_token)): 
    return upsert_generico(schema, "usuario_pdv", await request.json())

# --- ROTAS DE MOVIMENTO ---
@router.post("/sync/saida")
async def sync_saida(request: Request, schema: str = Depends(validar_token)): 
    return upsert_generico(schema, "saida", await request.json())

@router.post("/sync/saida_produto")
async def sync_saida_produto(request: Request, schema: str = Depends(validar_token)): 
    return upsert_generico(schema, "saida_produto", await request.json())

@router.post("/sync/saida_formapag")
async def sync_saida_formapag(request: Request, schema: str = Depends(validar_token)): 
    return upsert_generico(schema, "saida_formapag", await request.json())