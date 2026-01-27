from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from database_utils import get_db_connection, get_sql_novo_cliente
import secrets
import os

router = APIRouter()

# Pega a senha mestre do docker-compose (ou usa padrão se não existir)
SENHA_MESTRA = os.getenv("SENHA_ADMIN_SISTEMA", "SenhaParaCriarNovosClientes")

class NovaLoja(BaseModel):
    nome_fantasia: str
    cnpj: str
    senha_admin: str # Senha para autorizar a criação (SENHA_ADMIN_SISTEMA)

@router.post("/admin/criar-cliente")
def criar_cliente(loja: NovaLoja):
    """
    Cria o Schema do cliente e gera um Token de acesso.
    """
    # 1. Validação de Segurança
    if loja.senha_admin != SENHA_MESTRA:
        raise HTTPException(401, "Senha de administrador incorreta.")

    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Remove caracteres especiais do CNPJ
    cnpj_limpo = ''.join(filter(str.isdigit, loja.cnpj))
    nome_schema = f"tenant_{cnpj_limpo}"
    
    # Gera um token seguro para a loja (o que vai no config.ini)
    novo_token_loja = secrets.token_hex(32)
    
    try:
        # 2. Verifica duplicidade
        cursor.execute("SELECT id FROM lojas_sincronizadas WHERE cnpj = %s", (loja.cnpj,))
        if cursor.fetchone():
            raise HTTPException(400, "Já existe uma loja com este CNPJ.")

        # 3. Cria o Schema e as Tabelas
        sql_schema = get_sql_novo_cliente(nome_schema)
        cursor.execute(sql_schema)
        
        # 4. Registra na tabela Mestra
        cursor.execute("""
            INSERT INTO lojas_sincronizadas (nome_fantasia, cnpj, api_token, schema_name)
            VALUES (%s, %s, %s, %s)
        """, (loja.nome_fantasia, loja.cnpj, novo_token_loja, nome_schema))
        
        conn.commit()
        
        return {
            "status": "sucesso", 
            "mensagem": "Loja criada com sucesso!",
            "schema": nome_schema, 
            "token_acesso": novo_token_loja # <--- Copie este token para o config.ini
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        conn.rollback()
        print(f"Erro ao criar loja: {e}")
        raise HTTPException(500, detail=str(e))
    finally:
        conn.close()