from fastapi import APIRouter, Depends
from security import validar_token
from database_utils import get_db_connection

router = APIRouter()

@router.get("/admin/verificar-integridade")
def verificar_integridade(schema: str = Depends(validar_token)):
    conn = get_db_connection()
    cursor = conn.cursor()
    relatorio = { "status": "ok", "schema": schema, "erros": [] }

    try:
        # 1. Produtos Inexistentes
        cursor.execute(f"""
            SELECT sp.id_saida, sp.id_produto
            FROM {schema}.saida_produto sp
            LEFT JOIN {schema}.produto p ON sp.id_produto = p.id_original
            WHERE p.id_original IS NULL LIMIT 50
        """)
        for r in cursor.fetchall():
            relatorio["erros"].append({
                "tipo": "PRODUTO_INEXISTENTE", "acao": "REENVIAR_PRODUTO", "id_alvo": r[1], 
                "msg": f"Produto {r[1]} não cadastrado (Venda {r[0]})"
            })

        # 2. Formas Pagto Inexistentes
        cursor.execute(f"""
            SELECT sf.id_saida, sf.id_formapag 
            FROM {schema}.saida_formapag sf
            LEFT JOIN {schema}.formapag f ON sf.id_formapag = f.id_original
            WHERE f.id_original IS NULL LIMIT 50
        """)
        for r in cursor.fetchall():
            relatorio["erros"].append({
                "tipo": "PAGAMENTO_INVALIDO", "acao": "REENVIAR_FORMAPAG", "id_alvo": r[1],
                "msg": f"Forma Pagto {r[1]} não cadastrada"
            })

        # 3. Itens Órfãos (Sem Venda Pai)
        cursor.execute(f"""
            SELECT sp.id_original, sp.id_saida 
            FROM {schema}.saida_produto sp
            LEFT JOIN {schema}.saida s ON sp.id_saida = s.id_original
            WHERE s.id_original IS NULL LIMIT 50
        """)
        for r in cursor.fetchall():
            relatorio["erros"].append({
                "tipo": "ITEM_SEM_VENDA", "acao": "CHECK_VENDA_ELIMINADA", "id_alvo": r[1],
                "msg": f"Item {r[0]} aponta para venda inexistente {r[1]}"
            })

        # 4. Vendas Vazias (Sem Itens)
        cursor.execute(f"""
            SELECT s.id_original FROM {schema}.saida s
            LEFT JOIN {schema}.saida_produto sp ON s.id_original = sp.id_saida
            WHERE sp.id_original IS NULL AND (s.eliminado IS NULL OR s.eliminado = 'N') LIMIT 50
        """)
        for r in cursor.fetchall():
            relatorio["erros"].append({
                "tipo": "VENDA_SEM_ITENS", "acao": "REENVIAR_ITENS_VENDA", "id_alvo": r[0],
                "msg": f"Venda {r[0]} ativa mas sem itens"
            })
            
        # 5. Vendas sem Pagamento
        cursor.execute(f"""
            SELECT s.id_original FROM {schema}.saida s
            LEFT JOIN {schema}.saida_formapag sf ON s.id_original = sf.id_saida
            WHERE sf.id_original IS NULL AND (s.eliminado IS NULL OR s.eliminado = 'N') LIMIT 50
        """)
        for r in cursor.fetchall():
            relatorio["erros"].append({
                "tipo": "VENDA_SEM_PAGAMENTO", "acao": "REENVIAR_PAGTOS_VENDA", "id_alvo": r[0],
                "msg": f"Venda {r[0]} ativa mas sem pagamento"
            })

        # 6. NOVO: Vendas Eliminadas com Lixo (Itens/Pagtos)
        cursor.execute(f"""
            SELECT DISTINCT s.id_original FROM {schema}.saida s
            JOIN {schema}.saida_produto sp ON s.id_original = sp.id_saida
            WHERE s.eliminado = 'S' LIMIT 50
        """)
        for r in cursor.fetchall():
            relatorio["erros"].append({
                "tipo": "LIXO_VENDA_ELIMINADA", "acao": "FORCAR_DELECAO", "id_alvo": r[0],
                "msg": f"Venda {r[0]} está eliminada mas ainda tem itens."
            })

        if relatorio["erros"]:
            relatorio["status"] = "erro_integridade"
            relatorio["total_erros"] = len(relatorio["erros"])
        
        return relatorio
    except Exception as e: return {"status": "erro", "msg": str(e)}
    finally: conn.close()