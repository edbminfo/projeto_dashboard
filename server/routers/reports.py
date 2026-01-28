from fastapi import APIRouter, Depends
from typing import List, Optional
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

def verificar_tabela(cursor, schema, tabela, coluna=None):
    try:
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema='{schema}' AND table_name='{tabela}'"
        cursor.execute(sql)
        if not cursor.fetchone(): return False
        if coluna:
            sql_col = f"SELECT 1 FROM information_schema.columns WHERE table_schema='{schema}' AND table_name='{tabela}' AND column_name='{coluna}'"
            cursor.execute(sql_col)
            return cursor.fetchone() is not None
        return True
    except: return False

@router.get("/reports/dashboard-cards", response_model=DashboardCards)
def get_dashboard_cards(data_inicio: date, data_fim: date, schema: str = Depends(validar_token)):
    conn = get_db_connection(); cursor = conn.cursor()
    if not verificar_tabela(cursor, schema, 'saida', 'total'):
        conn.close(); return {k:0 for k in DashboardCards.__annotations__}

    sql_capa = f"""
        SELECT 
            COALESCE(SUM(NULLIF(total, '')::numeric), 0),
            COUNT(*),
            COALESCE(MAX(NULLIF(total, '')::numeric), 0),
            COALESCE(MIN(NULLIF(total, '')::numeric), 0)
        FROM {schema}.saida
        WHERE "data"::date BETWEEN %s AND %s AND (eliminado IS NULL OR eliminado = 'N')
    """
    
    sql_itens = f"""
        SELECT 
            COALESCE(SUM(NULLIF(sp.quant, '')::numeric), 0),
            COALESCE(SUM(NULLIF(sp.quant, '')::numeric * COALESCE(NULLIF(p.custo_total, '')::numeric, 0)), 0)
        FROM {schema}.saida_produto sp
        JOIN {schema}.saida s ON sp.id_saida = s.id_original
        LEFT JOIN {schema}.produto p ON sp.id_produto = p.id_original
        WHERE s."data"::date BETWEEN %s AND %s AND (s.eliminado IS NULL OR s.eliminado = 'N')
    """
    
    try:
        cursor.execute(sql_capa, (data_inicio, data_fim))
        capa = cursor.fetchone()
        fat, qtd, maior, menor = float(capa[0]), int(capa[1]), float(capa[2]), float(capa[3])
        
        qtd_itens, cmv = 0.0, 0.0
        if verificar_tabela(cursor, schema, 'saida_produto', 'quant'):
            cursor.execute(sql_itens, (data_inicio, data_fim))
            itens = cursor.fetchone()
            if itens: qtd_itens, cmv = float(itens[0]), float(itens[1])

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
        print(f"Erro Reports: {e}"); return {k:0 for k in DashboardCards.__annotations__}
    finally: conn.close()

@router.get("/reports/ranking/{tipo}", response_model=List[RankingItem])
def get_ranking(tipo: str, data_inicio: date, data_fim: date, limit: int = 20, schema: str = Depends(validar_token)):
    conn = get_db_connection(); cursor = conn.cursor()
    if not verificar_tabela(cursor, schema, 'saida', 'total'): conn.close(); return []

    sql = ""
    where_saida = f'WHERE s."data"::date BETWEEN %s AND %s AND (s.eliminado IS NULL OR s.eliminado = \'N\')'
    
    try:
        if tipo == "produto":
            if verificar_tabela(cursor, schema, 'produto'):
                sql = f"""
                    SELECT COALESCE(p.nome, 'N/D'), SUM(NULLIF(sp.total, '')::numeric), SUM(NULLIF(sp.quant, '')::numeric)
                    FROM {schema}.saida_produto sp
                    JOIN {schema}.saida s ON sp.id_saida = s.id_original
                    LEFT JOIN {schema}.produto p ON sp.id_produto = p.id_original
                    {where_saida} GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}
                """
        elif tipo == "hora":
            sql = f"""SELECT EXTRACT(HOUR FROM s."data")::text || 'h', SUM(NULLIF(s.total, '')::numeric), COUNT(*) FROM {schema}.saida s {where_saida} GROUP BY 1 ORDER BY 1 ASC"""
        elif tipo == "dia":
            sql = f"""SELECT TO_CHAR(s."data", 'DD/MM/YYYY'), SUM(NULLIF(s.total, '')::numeric), COUNT(*) FROM {schema}.saida s {where_saida} GROUP BY s."data"::date, 1 ORDER BY s."data"::date ASC"""
        elif tipo == "pagamento":
            if verificar_tabela(cursor, schema, 'saida_formapag'):
                nome_col = "sf.id_formapag"
                join_forma = ""
                if verificar_tabela(cursor, schema, 'formapag'):
                    join_forma = f"LEFT JOIN {schema}.formapag f ON sf.id_formapag = f.id_original"
                    nome_col = "COALESCE(f.nome, sf.id_formapag)"
                sql = f"""SELECT {nome_col}, SUM(NULLIF(sf.valor, '')::numeric), COUNT(DISTINCT s.id_original) FROM {schema}.saida_formapag sf JOIN {schema}.saida s ON sf.id_saida = s.id_original {join_forma} {where_saida} GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}"""
        
        # --- NOVO: VENDAS POR TERMINAL ---
        elif tipo == "terminal":
            if verificar_tabela(cursor, schema, 'saida', 'terminal'):
                sql = f"""
                    SELECT COALESCE(s.terminal, 'N/D'), SUM(NULLIF(s.total, '')::numeric), COUNT(*) 
                    FROM {schema}.saida s 
                    {where_saida} 
                    GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}
                """
                
        # --- NOVO: VENDAS POR USUARIO ---
        elif tipo == "usuario":
            if verificar_tabela(cursor, schema, 'saida', 'id_usuario'):
                col_nome = "s.id_usuario"
                join_user = ""
                # Se existir a tabela de usuários do PDV, tenta buscar o nome
                if verificar_tabela(cursor, schema, 'usuario_pdv', 'nome'):
                    join_user = f"LEFT JOIN {schema}.usuario_pdv u ON s.id_usuario = u.id_original"
                    col_nome = "COALESCE(u.nome, s.id_usuario)"
                
                sql = f"""
                    SELECT {col_nome}, SUM(NULLIF(s.total, '')::numeric), COUNT(*) 
                    FROM {schema}.saida s 
                    {join_user}
                    {where_saida} 
                    GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}
                """

        # Demais relatórios (Secao, Grupo, etc) mantidos implicitamente pela lógica anterior,
        # adicionei apenas os novos acima para brevidade, mas o arquivo completo deve ter todos.
        # ... (insira aqui os blocos secao, grupo, fabricante, fornecedor, cliente, vendedor do código anterior) ...
        # (Vou colocar todos aqui para garantir que você copie e cole e funcione tudo)
        
        elif tipo == "secao":
            if verificar_tabela(cursor, schema, 'secao'):
                sql = f"""SELECT COALESCE(sec.nome, 'N/D'), SUM(NULLIF(sp.total, '')::numeric), SUM(NULLIF(sp.quant, '')::numeric) FROM {schema}.saida_produto sp JOIN {schema}.saida s ON sp.id_saida = s.id_original LEFT JOIN {schema}.produto p ON sp.id_produto = p.id_original LEFT JOIN {schema}.grupo g ON p.id_grupo = g.id_original LEFT JOIN {schema}.secao sec ON g.id_secao = sec.id_original {where_saida} GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}"""
        elif tipo == "grupo":
            if verificar_tabela(cursor, schema, 'grupo'):
                sql = f"""SELECT COALESCE(g.nome, 'N/D'), SUM(NULLIF(sp.total, '')::numeric), SUM(NULLIF(sp.quant, '')::numeric) FROM {schema}.saida_produto sp JOIN {schema}.saida s ON sp.id_saida = s.id_original LEFT JOIN {schema}.produto p ON sp.id_produto = p.id_original LEFT JOIN {schema}.grupo g ON p.id_grupo = g.id_original {where_saida} GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}"""
        elif tipo == "fabricante":
            if verificar_tabela(cursor, schema, 'fabricante'):
                sql = f"""SELECT COALESCE(fab.nome, 'N/D'), SUM(NULLIF(sp.total, '')::numeric), SUM(NULLIF(sp.quant, '')::numeric) FROM {schema}.saida_produto sp JOIN {schema}.saida s ON sp.id_saida = s.id_original LEFT JOIN {schema}.produto p ON sp.id_produto = p.id_original LEFT JOIN {schema}.fabricante fab ON p.id_fabricante = fab.id_original {where_saida} GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}"""
        elif tipo == "fornecedor":
            if verificar_tabela(cursor, schema, 'cliente'):
                sql = f"""SELECT COALESCE(f.nome, 'N/D'), SUM(NULLIF(sp.total, '')::numeric), SUM(NULLIF(sp.quant, '')::numeric) FROM {schema}.saida_produto sp JOIN {schema}.saida s ON sp.id_saida = s.id_original LEFT JOIN {schema}.produto p ON sp.id_produto = p.id_original LEFT JOIN {schema}.cliente f ON p.id_fornecedor = f.id_original {where_saida} GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}"""
        elif tipo == "cliente":
            if verificar_tabela(cursor, schema, 'cliente'):
                sql = f"""SELECT COALESCE(c.nome, 'CONSUMIDOR'), SUM(NULLIF(s.total, '')::numeric), COUNT(*) FROM {schema}.saida s LEFT JOIN {schema}.cliente c ON s.id_cliente = c.id_original {where_saida} GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}"""
        elif tipo == "vendedor":
            if verificar_tabela(cursor, schema, 'vendedor'):
                sql = f"""SELECT COALESCE(v.nome, 'N/D'), SUM(NULLIF(s.total, '')::numeric), COUNT(*) FROM {schema}.saida s LEFT JOIN {schema}.vendedor v ON s.id_vendedor = v.id_original {where_saida} GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}"""

        if sql:
            cursor.execute(sql, (data_inicio, data_fim))
            return [{"nome": str(r[0]), "total": float(r[1]), "qtd": float(r[2])} for r in cursor.fetchall()]
        
        return []

    except Exception as e:
        print(f"Erro Ranking {tipo}: {e}"); return []
    finally: conn.close()