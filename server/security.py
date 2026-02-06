from fastapi import Header, HTTPException
from database_utils import get_db_connection

# Arquivo: server/security.py

async def validar_token(authorization: str = Header(...)):
    """
    Verifica se o Token existe E se a loja está ativa.
    """
    if not authorization.startswith("Bearer "):
        raise HTTPException(401, "Formato do token inválido (Use Bearer <token>)")
    
    token = authorization.replace("Bearer ", "")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # CORREÇÃO: Adicionado "AND ativo = TRUE" na consulta
    cursor.execute("""
        SELECT schema_name 
        FROM lojas_sincronizadas 
        WHERE api_token = %s AND ativo = TRUE
    """, (token,))
    
    resultado = cursor.fetchone()
    conn.close()
    
    if not resultado:
        # Agora essa mensagem serve tanto para token errado quanto para loja inativa
        raise HTTPException(status_code=401, detail="Acesso negado: Token inválido ou Loja inativa")
    
    return resultado[0]