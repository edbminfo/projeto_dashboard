import requests
import json

# --- CONFIGURAÇÃO ---
# Se estiver rodando do seu PC:
#URL_BASE = "https://api-dash.bmhelp.click"
# Se estiver rodando de dentro do servidor via SSH:
URL_BASE = "http://localhost:8000"

ROTA = "/api/admin/criar-cliente"

# Dados do Cliente que você quer criar
payload = {
    "cnpj": "40234567000199",           # Coloque um CNPJ válido (só números)
    "nome_fantasia": "Mercadinho Teste",
    "senha_admin": "SenhaParaCriarNovosClientes" # TEM QUE SER IGUAL AO DOCKER-COMPOSE
}

print(f"Tentando criar cliente em: {URL_BASE}{ROTA}...")

try:
    response = requests.post(f"{URL_BASE}{ROTA}", json=payload, timeout=10)
    
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        dados = response.json()
        print("\n✅ SUCESSO! Cliente criado.")
        print("="*40)
        print(f"TOKEN DA LOJA: {dados.get('token_acesso')}")
        print(f"SCHEMA BANCO:  {dados.get('schema')}")
        print("="*40)
        print("Agora coloque esse TOKEN no config.ini do cliente.")
    else:
        print("\n❌ ERRO NA CRIAÇÃO:")
        print(response.text) # Mostra o motivo do erro (senha errada, cnpj duplicado, etc)

except requests.exceptions.ConnectionError:
    print("\n❌ ERRO DE CONEXÃO: Não foi possível conectar na API.")
    print("Verifique se a URL está certa e se o servidor está no ar.")
except Exception as e:
    print(f"\n❌ ERRO INESPERADO: {e}")