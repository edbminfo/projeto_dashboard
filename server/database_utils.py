import psycopg2
import os
import time

# --- CONFIGURAÇÃO ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "senha123")
DB_DSN = f"postgres://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

def get_db_connection():
    max_retries = 15
    for i in range(max_retries):
        try:
            return psycopg2.connect(DB_DSN)
        except psycopg2.OperationalError:
            time.sleep(2)
    raise Exception("Não foi possível conectar ao banco após várias tentativas.")

def get_sql_novo_cliente(nome_schema):
    """
    Define a estrutura inicial usando os nomes originais do banco Firebird.
    """
    return f"""
    CREATE SCHEMA IF NOT EXISTS {nome_schema};

    -- 1. Tabela de Vendas (Capa)
    CREATE TABLE IF NOT EXISTS {nome_schema}.saida (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE
    );

    -- 2. Tabela de Itens (Produtos da Venda - Histórico)
    CREATE TABLE IF NOT EXISTS {nome_schema}.saida_produto (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        id_saida VARCHAR(50),
        id_produto VARCHAR(50)
    );
    CREATE INDEX IF NOT EXISTS idx_sp_saida ON {nome_schema}.saida_produto (id_saida);
    CREATE INDEX IF NOT EXISTS idx_sp_produto ON {nome_schema}.saida_produto (id_produto);

    -- 3. Cadastro de Formas de Pagamento
    CREATE TABLE IF NOT EXISTS {nome_schema}.formapag (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        nome VARCHAR(200),
        tipo VARCHAR(50)
    );

    -- 4. Vínculo Venda <-> Pagamento
    CREATE TABLE IF NOT EXISTS {nome_schema}.saida_formapag (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_saida VARCHAR(50),
        id_formapag VARCHAR(50),
        valor DECIMAL(18,4) -- Aumentado
    );
    CREATE INDEX IF NOT EXISTS idx_sf_saida ON {nome_schema}.saida_formapag (id_saida);

    -- 5. Outros Cadastros
    CREATE TABLE IF NOT EXISTS {nome_schema}.clientes (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        nome VARCHAR(200),
        cpf_cnpj VARCHAR(20),
        cidade VARCHAR(100),
        ativo VARCHAR(1)
    );

    CREATE TABLE IF NOT EXISTS {nome_schema}.vendedores (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        nome VARCHAR(100),
        comissao DECIMAL(10,4),
        ativo VARCHAR(1)
    );
    
    CREATE TABLE IF NOT EXISTS {nome_schema}.secoes (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        nome VARCHAR(100)
    );
    
    CREATE TABLE IF NOT EXISTS {nome_schema}.grupos (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        nome VARCHAR(100),
        id_secao VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS {nome_schema}.produtos (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        nome VARCHAR(200),
        preco_venda DECIMAL(18,4), -- Aumentado de (10,2) para (18,4)
        custo_total DECIMAL(18,4), -- Aumentado de (10,2) para (18,4)
        id_grupo VARCHAR(50),
        ativo VARCHAR(1)
    );
    """

def init_master_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS lojas_sincronizadas (
            id SERIAL PRIMARY KEY,
            nome_fantasia VARCHAR(100),
            cnpj VARCHAR(20),
            api_token VARCHAR(64) UNIQUE NOT NULL,
            schema_name VARCHAR(50) NOT NULL,
            criado_em TIMESTAMP DEFAULT NOW()
        );
        """)
        conn.commit()
    except Exception as e:
        print(f"Erro master table: {e}")
    finally:
        conn.close()