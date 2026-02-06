from fastapi import APIRouter, HTTPException, Header, Depends
from pydantic import BaseModel
from typing import List, Optional
from database_utils import get_db_connection, get_sql_novo_cliente
import secrets
import re
import os

router = APIRouter()

# --- CONFIGURAÇÃO DE SEGURANÇA ---
# Defina a senha aqui ou nas variáveis de ambiente do sistema
SENHA_ADMIN_SISTEMA = os.getenv("SENHA_ADMIN_SISTEMA", "admin123")

def verificar_admin(x_senha_admin: str = Header(..., alias="x-senha-admin")):
    """
    Verifica se o cabeçalho 'x-senha-admin' corresponde à senha do sistema.
    """
    if x_senha_admin != SENHA_ADMIN_SISTEMA:
        raise HTTPException(status_code=403, detail="Acesso negado: Senha de Admin incorreta")
    return x_senha_admin


# --- MODELO PARA REMOÇÃO DE VÍNCULO ---
class UsuarioLojaSchema(BaseModel):
    cnpj: str
    telefone: str

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

class StatusClienteSchema(BaseModel):
    cnpj: str
    ativo: bool

# --- ROTAS ---

# 1. LISTAR TOKENS (Protegida)
@router.get("/admin/listar-tokens", dependencies=[Depends(verificar_admin)])
def listar_tokens():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT nome_fantasia, cnpj, api_token, schema_name, ativo 
            FROM public.lojas_sincronizadas 
            ORDER BY nome_fantasia
        """)
        
        lista_lojas = []
        for linha in cursor.fetchall():
            lista_lojas.append({
                "nome_fantasia": linha[0],
                "cnpj": linha[1],
                "api_token": linha[2],
                "schema": linha[3],
                "ativo": linha[4]
            })
            
        return lista_lojas

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# 2. ALTERAR STATUS (Protegida)
@router.put("/admin/alterar-status", dependencies=[Depends(verificar_admin)])
def alterar_status_cliente(dados: StatusClienteSchema):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cnpj_limpo = re.sub(r'\D', '', dados.cnpj)
        
        cursor.execute("""
            UPDATE public.lojas_sincronizadas 
            SET ativo = %s 
            WHERE cnpj = %s
            RETURNING id, nome_fantasia
        """, (dados.ativo, cnpj_limpo))
        
        resultado = cursor.fetchone()
        conn.commit()
        
        if resultado:
            status_str = "ativada" if dados.ativo else "desativada"
            return {
                "status": "sucesso", 
                "mensagem": f"Loja '{resultado[1]}' {status_str} com sucesso.",
                "id_loja": resultado[0]
            }
        else:
            raise HTTPException(status_code=404, detail="Loja não encontrada com este CNPJ")

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# 3. CRIAR CLIENTE (Opcional: você também pode proteger esta rota se quiser)
@router.post("/admin/criar-cliente")
def criar_cliente(dados: NovoClienteSchema):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cnpj_limpo = re.sub(r'\D', '', dados.cnpj)
        if not cnpj_limpo: raise HTTPException(status_code=400, detail="CNPJ inválido")
            
        schema_name = f"tenant_{cnpj_limpo}"
        token_gerado = secrets.token_hex(32)
        
        # Cria Schema
        sql_init = get_sql_novo_cliente(schema_name)
        cursor.execute(sql_init)
        
        # Insere na Mestre
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
        
        # Cria Usuário Admin (Opcional)
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

# --- NOVA ROTA 1: LISTAR USUÁRIOS POR CNPJ ---
@router.get("/admin/usuarios-loja/{cnpj}", dependencies=[Depends(verificar_admin)])
def listar_usuarios_por_cnpj(cnpj: str):
    """
    Lista todos os usuários vinculados a um CNPJ específico.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cnpj_limpo = re.sub(r'\D', '', cnpj)
        
        cursor.execute("""
            SELECT u.id, u.nome, u.telefone 
            FROM public.usuarios u
            JOIN public.usuarios_lojas ul ON u.id = ul.usuario_id
            JOIN public.lojas_sincronizadas l ON ul.loja_id = l.id
            WHERE l.cnpj = %s
        """, (cnpj_limpo,))
        
        resultados = cursor.fetchall()
        
        lista_usuarios = []
        for linha in resultados:
            lista_usuarios.append({
                "id": linha[0],
                "nome": linha[1],
                "telefone": linha[2]
            })
            
        return lista_usuarios

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# --- NOVA ROTA 2: DESATIVAR (REMOVER) USUÁRIO DA LOJA ---
@router.delete("/admin/remover-usuario-loja", dependencies=[Depends(verificar_admin)])
def remover_usuario_da_loja(dados: UsuarioLojaSchema):
    """
    Remove o acesso de um usuário (telefone) a uma loja específica (CNPJ).
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cnpj_limpo = re.sub(r'\D', '', dados.cnpj)
        fone_limpo = re.sub(r'\D', '', dados.telefone)

        # Verifica se a loja existe
        cursor.execute("SELECT id FROM public.lojas_sincronizadas WHERE cnpj = %s", (cnpj_limpo,))
        loja = cursor.fetchone()
        if not loja:
            raise HTTPException(status_code=404, detail="Loja não encontrada")
        id_loja = loja[0]

        # Verifica se o usuário existe
        cursor.execute("SELECT id FROM public.usuarios WHERE telefone = %s", (fone_limpo,))
        usuario = cursor.fetchone()
        if not usuario:
            raise HTTPException(status_code=404, detail="Usuário não encontrado")
        id_usuario = usuario[0]

        # Remove o vínculo
        cursor.execute("""
            DELETE FROM public.usuarios_lojas 
            WHERE usuario_id = %s AND loja_id = %s
        """, (id_usuario, id_loja))
        
        if cursor.rowcount == 0:
             raise HTTPException(status_code=404, detail="Este usuário não estava vinculado a esta loja.")

        conn.commit()
        return {"status": "sucesso", "mensagem": f"Acesso do telefone {fone_limpo} removido da loja {cnpj_limpo}"}

    except HTTPException as he:
        raise he
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()