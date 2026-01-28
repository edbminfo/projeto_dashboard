from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from database_utils import get_db_connection, get_sql_novo_cliente
import secrets
import re

router = APIRouter()

# --- MODELOS ---
class LojaSchema(BaseModel):
    id: int
    nome_fantasia: str
    cnpj: str
    schema_name: str
    api_token: Optional[str] = None
    ativo: bool

class NovoClienteSchema(BaseModel):
    cnpj: str
    nome_fantasia: str
    senha_admin: str 
    telefone: Optional[str] = None

# --- ROTAS ---
@router.post("/admin/criar-cliente")
def criar_cliente(dados: NovoClienteSchema):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Validação
        cnpj_limpo = re.sub(r'\D', '', dados.cnpj)
        if not cnpj_limpo: raise HTTPException(status_code=400, detail="CNPJ inválido")
            
        schema_name = f"tenant_{cnpj_limpo}"
        token_gerado = secrets.token_hex(32)
        
        # 1. Cria Schema e Tabelas (Usando o SQL atualizado do database_utils)
        sql_init = get_sql_novo_cliente(schema_name)
        cursor.execute(sql_init)
        
        # 2. Insere na tabela Mestre
        cursor.execute("""
            INSERT INTO public.lojas_sincronizadas 
            (nome_fantasia, cnpj, schema_name, api_token, ativo)
            VALUES (%s, %s, %s, %s, TRUE)
            ON CONFLICT (cnpj) 
            DO UPDATE SET 
                api_token = EXCLUDED.api_token, 
                schema_name = EXCLUDED.schema_name,
                nome_fantasia = EXCLUDED.nome_fantasia
            RETURNING id
        """, (dados.nome_fantasia, cnpj_limpo, schema_name, token_gerado))
        
        loja_id = cursor.fetchone()[0]
        
        # 3. Cria Usuário Admin (Opcional)
        if dados.telefone:
            fone_limpo = re.sub(r'\D', '', dados.telefone)
            if fone_limpo:
                cursor.execute("""
                    INSERT INTO public.usuarios (nome, telefone) 
                    VALUES (%s, %s)
                    ON CONFLICT (telefone) DO UPDATE SET nome = EXCLUDED.nome
                    RETURNING id
                """, (f"Admin {dados.nome_fantasia}", fone_limpo))
                user_id = cursor.fetchone()[0]
                
                cursor.execute("""
                    INSERT INTO public.usuarios_lojas (usuario_id, loja_id)
                    VALUES (%s, %s)
                    ON CONFLICT (usuario_id, loja_id) DO NOTHING
                """, (user_id, loja_id))

        conn.commit()
        return {
            "status": "sucesso",
            "loja_id": loja_id,
            "schema": schema_name,
            "token_acesso": token_gerado
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally: conn.close()