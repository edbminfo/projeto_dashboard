import fdb
import configparser
import os

# Lê configurações para conectar no banco
diretorio_base = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
config.read(os.path.join(diretorio_base, 'config.ini'))

DB_PATH = config['DATABASE']['caminho']
DB_USER = config['DATABASE']['usuario']
DB_PASS = config['DATABASE']['senha']
DB_HOST = config['DATABASE']['host']
DB_PORT = config['DATABASE']['port']

print(f"Conectando em: {DB_PATH}...")

try:
    con = fdb.connect(host=DB_HOST, port=int(DB_PORT), database=DB_PATH, user=DB_USER, password=DB_PASS, charset='WIN1252')
    cur = con.cursor()
    
    # Verifica quantos registros existem
    cur.execute("SELECT COUNT(*) FROM SYNC_CONTROL")
    qtd = cur.fetchone()[0]
    print(f"Existem {qtd} registros marcados como enviados.")
    
    if qtd > 0:
        confirmacao = input("Deseja apagar o histórico de envio e REENVIAR TUDO? (s/n): ")
        if confirmacao.lower() == 's':
            cur.execute("DELETE FROM SYNC_CONTROL")
            con.commit()
            print("Histórico limpo! Execute o agente_sync.py novamente.")
        else:
            print("Operação cancelada.")
    else:
        print("A tabela de controle já está vazia.")

    con.close()

except Exception as e:
    print(f"Erro: {e}")
    print("Se o erro for 'Table unknown', é porque a tabela nem foi criada ainda (tudo certo).")