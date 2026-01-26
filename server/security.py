from fastapi import Header, HTTPException
from database_utils import get_db_connection

async def validar_token(authorization: str = Header(...)):
    """
    Verifica se o Token existe e retorna o nome do SCHEMA do banco de dados.
    """
    if not authorization.startswith("Bearer "):
        raise HTTPException(401, "Formato do token inválido (Use Bearer <token>)")
    
    token = authorization.replace("Bearer ", "")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Busca qual schema pertence a este token
    cursor.execute("SELECT schema_name FROM lojas_sincronizadas WHERE api_token = %s", (token,))
    resultado = cursor.fetchone()
    conn.close()
    
    if not resultado:
        raise HTTPException(status_code=401, detail="Token de acesso inválido ou loja não cadastrada")
    
    return resultado[0] # Retorna string ex: "tenant_123456"