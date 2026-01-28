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
    Define a estrutura inicial completa para um novo cliente (Tenant).
    """
    return f"""
    CREATE SCHEMA IF NOT EXISTS {nome_schema};

    -- 1. Tabela de Vendas (Capa)
    CREATE TABLE IF NOT EXISTS {nome_schema}.saida (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        data TIMESTAMP,
        total TEXT,
        id_cliente VARCHAR(50),
        id_vendedor VARCHAR(50),
        terminal VARCHAR(50),      -- Novo: Caixa/Terminal
        id_usuario VARCHAR(50),    -- Novo: Operador do PDV
        eliminado VARCHAR(1),
        criado_em TIMESTAMP DEFAULT NOW(),
        modificado_em TIMESTAMP DEFAULT NOW()
    );

    -- 2. Tabela de Itens
    CREATE TABLE IF NOT EXISTS {nome_schema}.saida_produto (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        id_saida VARCHAR(50),
        id_produto VARCHAR(50),
        quant TEXT,
        total TEXT,
        criado_em TIMESTAMP DEFAULT NOW(),
        modificado_em TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_sp_saida ON {nome_schema}.saida_produto (id_saida);
    CREATE INDEX IF NOT EXISTS idx_sp_produto ON {nome_schema}.saida_produto (id_produto);

    -- 3. Formas de Pagamento
    CREATE TABLE IF NOT EXISTS {nome_schema}.formapag (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        nome VARCHAR(200),
        tipo VARCHAR(50),
        criado_em TIMESTAMP DEFAULT NOW(),
        modificado_em TIMESTAMP DEFAULT NOW()
    );

    -- 4. Vínculo Venda <-> Pagamento
    CREATE TABLE IF NOT EXISTS {nome_schema}.saida_formapag (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_saida VARCHAR(50),
        id_formapag VARCHAR(50),
        valor TEXT,
        criado_em TIMESTAMP DEFAULT NOW(),
        modificado_em TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_sf_saida ON {nome_schema}.saida_formapag (id_saida);

    -- 5. Cadastros Gerais
    CREATE TABLE IF NOT EXISTS {nome_schema}.cliente (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        nome VARCHAR(200),
        cnpj_cpf VARCHAR(20),
        cidade VARCHAR(100),
        ativo VARCHAR(1),
        criado_em TIMESTAMP DEFAULT NOW(),
        modificado_em TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS {nome_schema}.vendedor (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        nome VARCHAR(100),
        comissao TEXT,
        ativo VARCHAR(1),
        criado_em TIMESTAMP DEFAULT NOW(),
        modificado_em TIMESTAMP DEFAULT NOW()
    );
    
    CREATE TABLE IF NOT EXISTS {nome_schema}.usuario_pdv (  -- Novo: Tabela de Operadores
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        nome VARCHAR(100),
        login VARCHAR(50),
        criado_em TIMESTAMP DEFAULT NOW(),
        modificado_em TIMESTAMP DEFAULT NOW()
    );
    
    CREATE TABLE IF NOT EXISTS {nome_schema}.secao (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        nome VARCHAR(100),
        criado_em TIMESTAMP DEFAULT NOW(),
        modificado_em TIMESTAMP DEFAULT NOW()
    );
    
    CREATE TABLE IF NOT EXISTS {nome_schema}.grupo (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        nome VARCHAR(100),
        id_secao VARCHAR(50),
        criado_em TIMESTAMP DEFAULT NOW(),
        modificado_em TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS {nome_schema}.fabricante (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        nome VARCHAR(100),
        criado_em TIMESTAMP DEFAULT NOW(),
        modificado_em TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS {nome_schema}.familia (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        nome VARCHAR(100),
        criado_em TIMESTAMP DEFAULT NOW(),
        modificado_em TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS {nome_schema}.produto (
        uuid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_original VARCHAR(50) UNIQUE,
        nome VARCHAR(200),
        preco_venda TEXT,
        custo_total TEXT,
        id_grupo VARCHAR(50),
        id_fabricante VARCHAR(50),
        id_fornecedor VARCHAR(50),
        id_familia VARCHAR(50),
        ativo VARCHAR(1),
        criado_em TIMESTAMP DEFAULT NOW(),
        modificado_em TIMESTAMP DEFAULT NOW()
    );
    """

def init_master_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.lojas_sincronizadas (
            id SERIAL PRIMARY KEY,
            nome_fantasia VARCHAR(100),
            cnpj VARCHAR(20) UNIQUE,
            api_token VARCHAR(100),
            schema_name VARCHAR(50) NOT NULL,
            ativo BOOLEAN DEFAULT TRUE,
            criado_em TIMESTAMP DEFAULT NOW()
        );
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.usuarios (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100),
            telefone VARCHAR(20) UNIQUE,
            criado_em TIMESTAMP DEFAULT NOW()
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.usuarios_lojas (
            usuario_id INT REFERENCES public.usuarios(id),
            loja_id INT REFERENCES public.lojas_sincronizadas(id),
            PRIMARY KEY (usuario_id, loja_id)
        );
        """)
        
        conn.commit()
    except Exception as e:
        print(f"Erro master table: {e}")
    finally:
        conn.close()