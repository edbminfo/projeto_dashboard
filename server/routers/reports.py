from fastapi import APIRouter, Depends, HTTPException
from typing import List
from pydantic import BaseModel
from security import validar_token
from database_utils import get_db_connection
from datetime import date

router = APIRouter()

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

# Colunas do Firebird que chegam no Postgres
C_SAIDA_TOTAL = "total"
C_SAIDA_DATA = "data"
C_SP_TOTAL = "total"
C_SP_QTD = "quant"

@router.get("/reports/dashboard-cards", response_model=DashboardCards)
def get_dashboard_cards(data_inicio: date, data_fim: date, schema: str = Depends(validar_token)):
    conn = get_db_connection(); cursor = conn.cursor()
    
    # 1. Capa (Faturamento Realizado)
    sql_capa = f"""
        SELECT 
            COALESCE(SUM("{C_SAIDA_TOTAL}"), 0),
            COUNT(*),
            COALESCE(MAX("{C_SAIDA_TOTAL}"), 0),
            COALESCE(MIN("{C_SAIDA_TOTAL}"), 0)
        FROM {schema}.saida
        WHERE "{C_SAIDA_DATA}"::date BETWEEN %s AND %s
    """
    
    # 2. Itens (Para Custo/CMV e Qtde Produtos)
    # Usa Custo do cadastro atual do Produto (Padrão de mercado simplificado)
    sql_itens = f"""
        SELECT 
            COALESCE(SUM(sp."{C_SP_QTD}"), 0),
            COALESCE(SUM(sp."{C_SP_QTD}" * COALESCE(p.custo_total, 0)), 0)
        FROM {schema}.saida_produto sp
        JOIN {schema}.saida s ON sp.id_saida = s.id_original
        LEFT JOIN {schema}.produtos p ON sp.id_produto = p.id_original
        WHERE s."{C_SAIDA_DATA}"::date BETWEEN %s AND %s
    """
    
    try:
        cursor.execute(sql_capa, (data_inicio, data_fim))
        capa = cursor.fetchone()
        fat, qtd, maior, menor = float(capa[0]), int(capa[1]), float(capa[2]), float(capa[3])
        
        cursor.execute(sql_itens, (data_inicio, data_fim))
        itens = cursor.fetchone()
        qtd_itens, cmv = float(itens[0]), float(itens[1])
        
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
        print(f"Erro Cards: {e}"); return { "faturamento":0, "qtde_vendas":0, "ticket_medio":0, "itens_por_venda":0, "cmv":0, "lucro_bruto":0, "markup":0, "lucro_bruto_percent":0, "maior_venda":0, "menor_venda":0 }
    finally: conn.close()

@router.get("/reports/ranking/{tipo}", response_model=List[RankingItem])
def get_ranking(tipo: str, data_inicio: date, data_fim: date, limit: int = 10, schema: str = Depends(validar_token)):
    conn = get_db_connection(); cursor = conn.cursor()
    sql = ""
    where = f'WHERE s."{C_SAIDA_DATA}"::date BETWEEN %s AND %s'

    try:
        if tipo == "produto":
            sql = f"""
                SELECT COALESCE(p.nome, 'N/D'), SUM(sp."{C_SP_TOTAL}"), SUM(sp."{C_SP_QTD}")
                FROM {schema}.saida_produto sp
                JOIN {schema}.saida s ON sp.id_saida = s.id_original
                LEFT JOIN {schema}.produtos p ON sp.id_produto = p.id_original
                {where} GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}
            """
        elif tipo == "pagamento":
            sql = f"""
                SELECT COALESCE(fp.nome, 'N/D'), SUM(sf.valor), COUNT(DISTINCT s.id_original)
                FROM {schema}.saida_formapag sf
                JOIN {schema}.saida s ON sf.id_saida = s.id_original
                LEFT JOIN {schema}.formapag fp ON sf.id_formapag = fp.id_original
                {where} GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}
            """
        
        if sql:
            cursor.execute(sql, (data_inicio, data_fim))
            return [{"nome": str(r[0]), "total": float(r[1]), "qtd": float(r[2])} for r in cursor.fetchall()]
        return []
        
    except Exception as e:
        print(f"Erro Ranking {tipo}: {e}"); return []
    finally: conn.close()