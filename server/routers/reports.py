from fastapi import APIRouter, Depends
from typing import List, Optional
from pydantic import BaseModel
from security import validar_token
from database_utils import get_db_connection
from datetime import date

router = APIRouter()

# --- MODELS ---
class DashboardCards(BaseModel):
    faturamento: float
    qtde_vendas: int
    ticket_medio: float
    itens_por_venda: float
    cmv: float
    lucro_bruto: float
    markup: float
    lucro_bruto_percent: float
    maior_venda: float
    menor_venda: float

class RankingItem(BaseModel):
    nome: str
    total: float
    qtd: float

# --- UTILS ---
def verificar_tabela(cursor, schema, tabela, coluna):
    try:
        cursor.execute(f"SELECT 1 FROM information_schema.columns WHERE table_schema='{schema}' AND table_name='{tabela}' AND column_name='{coluna}'")
        return cursor.fetchone() is not None
    except: return False

# --- ENDPOINTS ---

@router.get("/reports/dashboard-cards", response_model=DashboardCards)
def get_dashboard_cards(data_inicio: date, data_fim: date, schema: str = Depends(validar_token)):
    conn = get_db_connection(); cursor = conn.cursor()
    
    # Validação inicial
    if not verificar_tabela(cursor, schema, 'saida', 'total'):
        conn.close()
        return {k:0 for k in DashboardCards.__annotations__}

    # 1. Dados da Venda (Capa)
    sql_capa = f"""
        SELECT 
            COALESCE(SUM(NULLIF(total, '')::numeric), 0),
            COUNT(*),
            COALESCE(MAX(NULLIF(total, '')::numeric), 0),
            COALESCE(MIN(NULLIF(total, '')::numeric), 0)
        FROM {schema}.saida
        WHERE "data"::date BETWEEN %s AND %s
    """
    
    # 2. Dados dos Itens (CMV e Qtde) - JOIN via id_original
    sql_itens = f"""
        SELECT 
            COALESCE(SUM(NULLIF(sp.quant, '')::numeric), 0),
            COALESCE(SUM(NULLIF(sp.quant, '')::numeric * COALESCE(NULLIF(p.custo_total, '')::numeric, 0)), 0)
        FROM {schema}.saida_produto sp
        JOIN {schema}.saida s ON sp.id_saida = s.id_original
        LEFT JOIN {schema}.produto p ON sp.id_produto = p.id_original
        WHERE s."data"::date BETWEEN %s AND %s
    """
    
    try:
        cursor.execute(sql_capa, (data_inicio, data_fim))
        capa = cursor.fetchone()
        fat, qtd, maior, menor = float(capa[0]), int(capa[1]), float(capa[2]), float(capa[3])
        
        qtd_itens, cmv = 0.0, 0.0
        if verificar_tabela(cursor, schema, 'saida_produto', 'quant'):
            cursor.execute(sql_itens, (data_inicio, data_fim))
            itens = cursor.fetchone()
            if itens:
                qtd_itens, cmv = float(itens[0]), float(itens[1])

        # Cálculos
        ticket = fat / qtd if qtd > 0 else 0.0
        itens_pv = qtd_itens / qtd if qtd > 0 else 0.0
        lucro = fat - cmv
        markup = (lucro / cmv * 100) if cmv > 0 else 0.0
        margem = (lucro / fat * 100) if fat > 0 else 0.0

        return {
            "faturamento": fat, "qtde_vendas": qtd, "ticket_medio": ticket,
            "itens_por_venda": itens_pv, "cmv": cmv, "lucro_bruto": lucro,
            "markup": markup, "lucro_bruto_percent": margem, "maior_venda": maior, "menor_venda": menor
        }
    except Exception as e:
        print(f"Erro Reports: {e}")
        return {k:0 for k in DashboardCards.__annotations__}
    finally: conn.close()

@router.get("/reports/ranking/{tipo}", response_model=List[RankingItem])
def get_ranking(tipo: str, data_inicio: date, data_fim: date, limit: int = 10, schema: str = Depends(validar_token)):
    conn = get_db_connection(); cursor = conn.cursor()
    if not verificar_tabela(cursor, schema, 'saida', 'total'): conn.close(); return []

    sql = ""
    where = f'WHERE s."data"::date BETWEEN %s AND %s'

    try:
        if tipo == "produto":
            if not verificar_tabela(cursor, schema, 'produto', 'nome'): return []
            sql = f"""
                SELECT COALESCE(p.nome, 'N/D'), SUM(NULLIF(sp.total, '')::numeric), SUM(NULLIF(sp.quant, '')::numeric)
                FROM {schema}.saida_produto sp
                JOIN {schema}.saida s ON sp.id_saida = s.id_original
                LEFT JOIN {schema}.produto p ON sp.id_produto = p.id_original
                {where} GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}
            """
        
        elif tipo == "pagamento":
            if not verificar_tabela(cursor, schema, 'saida_formapag', 'valor'): return []
            sql = f"""
                SELECT COALESCE(sf.id_formapag, 'N/D'), SUM(NULLIF(sf.valor, '')::numeric), COUNT(DISTINCT s.id_original)
                FROM {schema}.saida_formapag sf
                JOIN {schema}.saida s ON sf.id_saida = s.id_original
                {where} GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}
            """
        
        if sql:
            cursor.execute(sql, (data_inicio, data_fim))
            return [{"nome": str(r[0]), "total": float(r[1]), "qtd": float(r[2])} for r in cursor.fetchall()]
        return []
    except Exception as e:
        print(f"Erro Ranking {tipo}: {e}"); return []
    finally: conn.close()