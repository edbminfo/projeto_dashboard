from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from database_utils import get_db_connection
import secrets
import re

router = APIRouter()

# --- MODELOS ---
class LojaSchema(BaseModel):
    id: int
    nome_fantasia: str
    cnpj: str
    schema_name: str
    api_token: Optional[str] = None  # <== Voltamos para o nome original
    ativo: bool

class NovoClienteSchema(BaseModel):
    cnpj: str
    nome_fantasia: str
    senha_admin: str 
    telefone: Optional[str] = None

# --- FUNÇÃO DE AUTO-CORREÇÃO DE TABELAS ---
def garantir_estrutura_admin(cursor):
    """
    Garante que a tabela lojas_sincronizadas tenha a coluna api_token e o índice único.
    """
    
    # 1. Cria a tabela base se não existir (Usando api_token agora)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.lojas_sincronizadas (
            id SERIAL PRIMARY KEY,
            nome_fantasia VARCHAR(255) NOT NULL,
            cnpj VARCHAR(20) NOT NULL,
            schema_name VARCHAR(100) NOT NULL,
            api_token VARCHAR(100),       /* <== Nome corrigido para api_token */
            ativo BOOLEAN DEFAULT TRUE,
            criado_em TIMESTAMP DEFAULT NOW()
        )
    """)

    # 2. AUTO-FIX: Adiciona colunas faltantes se a tabela já existir
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
          AND table_name = 'lojas_sincronizadas'
    """)
    colunas_existentes = {row[0] for row in cursor.fetchall()}

    # Se faltar api_token, cria
    if 'api_token' not in colunas_existentes:
        print("   [ADMIN] Criando coluna 'api_token'...")
        cursor.execute("ALTER TABLE public.lojas_sincronizadas ADD COLUMN api_token VARCHAR(100)")
        
    # (Segurança) Se por acaso existir token_acesso (do código anterior), ignoramos ele e usamos api_token

    if 'ativo' not in colunas_existentes:
        cursor.execute("ALTER TABLE public.lojas_sincronizadas ADD COLUMN ativo BOOLEAN DEFAULT TRUE")

    if 'criado_em' not in colunas_existentes:
        cursor.execute("ALTER TABLE public.lojas_sincronizadas ADD COLUMN criado_em TIMESTAMP DEFAULT NOW()")

    # 3. AUTO-FIX CRÍTICO: Garante que o CNPJ seja ÚNICO
    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_lojas_cnpj_unico 
        ON public.lojas_sincronizadas (cnpj)
    """)

    # 4. Tabelas de Usuários
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.usuarios (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100),
            telefone VARCHAR(20) UNIQUE,
            criado_em TIMESTAMP DEFAULT NOW()
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.usuarios_lojas (
            usuario_id INT REFERENCES public.usuarios(id),
            loja_id INT REFERENCES public.lojas_sincronizadas(id),
            PRIMARY KEY (usuario_id, loja_id)
        )
    """)

# --- ROTAS DE CRIAÇÃO ---

@router.post("/admin/criar-cliente")
def criar_cliente(dados: NovoClienteSchema):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # >>>> Roda a correção <<<<
        garantir_estrutura_admin(cursor)
        
        # 1. Validação
        cnpj_limpo = re.sub(r'\D', '', dados.cnpj)
        if not cnpj_limpo:
            raise HTTPException(status_code=400, detail="CNPJ inválido")
            
        schema_name = f"tenant_{cnpj_limpo}"
        token_gerado = secrets.token_hex(32)
        
        # 2. Cria Schema
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        
        # 3. Insere Loja (Usando api_token)
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
        
        # 4. Cria Usuário (Opcional)
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
            "msg": "Cliente criado com sucesso!",
            "loja_id": loja_id,
            "schema": schema_name,
            "token_para_config_ini": token_gerado
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# --- ROTAS DE LEITURA ---

@router.get("/admin/lojas", response_model=List[LojaSchema])
def listar_lojas():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        garantir_estrutura_admin(cursor)
        conn.commit()

        # Busca api_token
        cursor.execute("""
            SELECT id, nome_fantasia, cnpj, schema_name, api_token, ativo 
            FROM public.lojas_sincronizadas 
            ORDER BY id
        """)
        lojas = cursor.fetchall()
        return [
            {
                "id": r[0], 
                "nome_fantasia": r[1], 
                "cnpj": r[2], 
                "schema_name": r[3],
                "api_token": r[4], # Mapeia para o modelo LojaSchema (api_token)
                "ativo": r[5]
            } 
            for r in lojas
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@router.get("/admin/check-schema/{schema_name}")
def check_schema_health(schema_name: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (schema_name,))
        if not cursor.fetchone():
            return {"status": "erro", "msg": "Schema não encontrado"}
            
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.saida")
            qtd = cursor.fetchone()[0]
            return {"status": "ok", "tabela": "saida", "qtd_registros": qtd}
        except:
            return {"status": "aviso", "msg": "Tabela 'saida' ainda não existe"}
            
    except Exception as e:
        return {"status": "erro", "msg": str(e)}
    finally:
        conn.close()