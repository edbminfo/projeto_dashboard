from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from database_utils import get_db_connection, get_sql_novo_cliente
# Importa passlib para gerar senha (token) segura se precisar, mas aqui usaremos texto puro por simplicidade no teste
# from passlib.context import CryptContext 

router = APIRouter()

class NovaLoja(BaseModel):
    nome_fantasia: str
    cnpj: str
    senha_token: str # A senha que a loja vai usar para logar

@router.post("/admin/criar-loja")
def criar_loja(loja: NovaLoja):
    """
    Cria o Schema do cliente e registra na tabela de login.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Define o nome do schema (ex: tenant_40234567000199)
    # Remove pontos e traços do CNPJ para ficar limpo
    cnpj_limpo = ''.join(filter(str.isdigit, loja.cnpj))
    nome_schema = f"tenant_{cnpj_limpo}"
    
    try:
        # 2. Cria o Schema e Tabelas (usando o utilitário que já fizemos)
        sql_schema = get_sql_novo_cliente(nome_schema)
        cursor.execute(sql_schema)
        
        # 3. Registra na tabela Mestra de Login (lojas_sincronizadas)
        # Verifica se já existe antes
        cursor.execute("SELECT id FROM lojas_sincronizadas WHERE cnpj = %s", (loja.cnpj,))
        if cursor.fetchone():
            raise HTTPException(400, "Loja com este CNPJ já existe.")
            
        cursor.execute("""
            INSERT INTO lojas_sincronizadas (nome_fantasia, cnpj, api_token, schema_name)
            VALUES (%s, %s, %s, %s)
        """, (loja.nome_fantasia, loja.cnpj, loja.senha_token, nome_schema))
        
        conn.commit()
        return {"status": "sucesso", "schema": nome_schema, "mensagem": "Loja criada e pronta para sincronizar!"}
        
    except Exception as e:
        conn.rollback()
        print(f"Erro ao criar loja: {e}")
        raise HTTPException(500, detail=str(e))
    finally:
        conn.close()