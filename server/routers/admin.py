from fastapi import APIRouter, HTTPException, Header, Depends
from pydantic import BaseModel
from typing import List, Optional
from database_utils import get_db_connection, get_sql_novo_cliente
import secrets
import re
import os

router = APIRouter()

# --- CONFIGURAÇÃO DE SEGURANÇA ---
# Defina a senha aqui ou nas variáveis de ambiente
SENHA_ADMIN_SISTEMA = os.getenv("SENHA_ADMIN_SISTEMA", "admin123")

def verificar_admin(x_senha_admin: str = Header(..., alias="x-senha-admin")):
    """
    Verifica se o cabeçalho 'x-senha-admin' corresponde à senha do sistema.
    """
    if x_senha_admin != SENHA_ADMIN_SISTEMA:
        raise HTTPException(status_code=403, detail="Acesso negado: Senha de Admin incorreta")
    return x_senha_admin

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

class UsuarioLojaSchema(BaseModel):
    cnpj: str
    telefone: str

# Modelo para o Webhook (Migrado do Front)
class WebhookUsuarioSchema(BaseModel):
    nome: str
    telefone: str
    cnpjs: List[str]
    admin_secret: str

# --- ROTAS ---

# 1. LISTAR TOKENS
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

# 2. ALTERAR STATUS
@router.put("/admin/alterar-status", dependencies=[Depends(verificar_admin)])
def alterar_status_cliente(dados: StatusClienteSchema):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cnpj_limpo = re.sub(r'\D', '', dados.cnpj)
        cursor.execute("UPDATE public.lojas_sincronizadas SET ativo = %s WHERE cnpj = %s RETURNING id, nome_fantasia", (dados.ativo, cnpj_limpo))
        resultado = cursor.fetchone()
        conn.commit()
        
        if resultado:
            status_str = "ativada" if dados.ativo else "desativada"
            return {"status": "sucesso", "mensagem": f"Loja '{resultado[1]}' {status_str}.", "id_loja": resultado[0]}
        else:
            raise HTTPException(status_code=404, detail="Loja não encontrada")
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# 3. LISTAR USUÁRIOS POR LOJA
@router.get("/admin/usuarios-loja/{cnpj}", dependencies=[Depends(verificar_admin)])
def listar_usuarios_por_cnpj(cnpj: str):
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
        
        return [{"id": r[0], "nome": r[1], "telefone": r[2]} for r in cursor.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# 4. REMOVER USUÁRIO DA LOJA
@router.delete("/admin/remover-usuario-loja", dependencies=[Depends(verificar_admin)])
def remover_usuario_da_loja(dados: UsuarioLojaSchema):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cnpj_limpo = re.sub(r'\D', '', dados.cnpj)
        fone_limpo = re.sub(r'\D', '', dados.telefone)

        # Busca IDs
        cursor.execute("SELECT id FROM public.lojas_sincronizadas WHERE cnpj = %s", (cnpj_limpo,))
        loja = cursor.fetchone()
        if not loja: raise HTTPException(404, "Loja não encontrada")

        cursor.execute("SELECT id FROM public.usuarios WHERE telefone = %s", (fone_limpo,))
        usuario = cursor.fetchone()
        if not usuario: raise HTTPException(404, "Usuário não encontrado")

        cursor.execute("DELETE FROM public.usuarios_lojas WHERE usuario_id = %s AND loja_id = %s", (usuario[0], loja[0]))
        if cursor.rowcount == 0: raise HTTPException(404, "Usuário não vinculado a esta loja")
        
        conn.commit()
        return {"status": "sucesso", "mensagem": f"Vínculo removido."}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        conn.close()

# 5. CRIAR CLIENTE (Novo Tenant)
@router.post("/admin/criar-cliente")
def criar_cliente(dados: NovoClienteSchema):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cnpj_limpo = re.sub(r'\D', '', dados.cnpj)
        if not cnpj_limpo: raise HTTPException(400, "CNPJ inválido")
            
        schema_name = f"tenant_{cnpj_limpo}"
        token_gerado = secrets.token_hex(32)
        
        # Cria Schema e Tabelas
        cursor.execute(get_sql_novo_cliente(schema_name))
        
        # Insere na Mestre
        cursor.execute("""
            INSERT INTO public.lojas_sincronizadas (nome_fantasia, cnpj, schema_name, api_token, ativo)
            VALUES (%s, %s, %s, %s, TRUE)
            ON CONFLICT (cnpj) DO UPDATE SET api_token = EXCLUDED.api_token, schema_name = EXCLUDED.schema_name, nome_fantasia = EXCLUDED.nome_fantasia
            RETURNING id
        """, (dados.nome_fantasia, cnpj_limpo, schema_name, token_gerado))
        loja_id = cursor.fetchone()[0]
        
        # Cria Admin (Opcional)
        if dados.telefone:
            fone_limpo = re.sub(r'\D', '', dados.telefone)
            if fone_limpo:
                cursor.execute("INSERT INTO public.usuarios (nome, telefone) VALUES (%s, %s) ON CONFLICT (telefone) DO UPDATE SET nome = EXCLUDED.nome RETURNING id", (f"Admin {dados.nome_fantasia}", fone_limpo))
                user_id = cursor.fetchone()[0]
                cursor.execute("INSERT INTO public.usuarios_lojas (usuario_id, loja_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, loja_id))

        conn.commit()
        return {"status": "sucesso", "loja_id": loja_id, "schema": schema_name, "token_acesso": token_gerado}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally: conn.close()

# 6. WEBHOOK: CRIAR USUÁRIO (Migrado do Front)
@router.post("/admin/criar-usuario")
def criar_usuario_webhook(dados: WebhookUsuarioSchema):
    """
    Rota utilizada pelo n8n ou sistemas externos para cadastrar usuários e vincular a múltiplos CNPJs.
    Mantém a verificação via 'admin_secret' no corpo da requisição para compatibilidade.
    """
    # Verifica a senha enviada no JSON (igual funcionava no Front)
    if dados.admin_secret != SENHA_ADMIN_SISTEMA:
        raise HTTPException(status_code=401, detail="Não autorizado: Secret incorreto")

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        fone_limpo = re.sub(r'\D', '', dados.telefone)
        
        # 1. Cria ou Atualiza o Usuário
        cursor.execute("""
            INSERT INTO public.usuarios (nome, telefone) 
            VALUES (%s, %s) 
            ON CONFLICT (telefone) DO UPDATE SET nome = EXCLUDED.nome 
            RETURNING id
        """, (dados.nome, fone_limpo))
        usuario_id = cursor.fetchone()[0]

        # 2. Busca os IDs das lojas baseados nos CNPJs enviados
        # O ANY(%s) no Python/Psycopg2 espera uma lista
        lista_cnpjs_limpos = [re.sub(r'\D', '', c) for c in dados.cnpjs]
        
        if not lista_cnpjs_limpos:
            return {"status": "aviso", "mensagem": "Usuário criado, mas nenhum CNPJ válido fornecido."}

        cursor.execute("""
            SELECT id, cnpj FROM public.lojas_sincronizadas 
            WHERE cnpj = ANY(%s)
        """, (lista_cnpjs_limpos,))
        
        lojas_encontradas = cursor.fetchall()

        # 3. Vincula o usuário a cada loja encontrada
        vinculos_criados = 0
        for loja in lojas_encontradas:
            loja_id = loja[0]
            cursor.execute("""
                INSERT INTO public.usuarios_lojas (usuario_id, loja_id) 
                VALUES (%s, %s) 
                ON CONFLICT (usuario_id, loja_id) DO NOTHING
            """, (usuario_id, loja_id))
            vinculos_criados += 1

        conn.commit()
        return {
            "status": "sucesso", 
            "usuario_id": usuario_id, 
            "lojas_vinculadas": vinculos_criados,
            "cnpjs_encontrados": [l[1] for l in lojas_encontradas]
        }

    except Exception as e:
        conn.rollback()
        print(f"Erro Webhook: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()