import requests
import json
import os
from datetime import datetime, timedelta

# --- CONFIGURAÇÃO ---
# Se estiver rodando local, use localhost. Se for remoto, coloque o IP.
API_URL = "http://localhost:8000/api"

# COLOQUE AQUI O TOKEN DA LOJA QUE VOCÊ CRIOU (O mesmo do config.ini)
TOKEN = "12c1fa4c10fadc4a26a7865f9b3ec2280de3da60140910951370d95e3b3c2552" 

def limpar_tela():
    os.system('cls' if os.name == 'nt' else 'clear')

def get_headers():
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

def print_json(dados):
    """Imprime o JSON colorido/formatado"""
    print(json.dumps(dados, indent=4, ensure_ascii=False))

def requisitar(endpoint, params=None):
    try:
        url = f"{API_URL}{endpoint}"
        print(f"\n🔄 Consultando: {url} ...")
        response = requests.get(url, headers=get_headers(), params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Erro {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"❌ Erro de Conexão: {e}")
        return None

def menu_datas():
    """Define datas padrão para facilitar"""
    hoje = datetime.now().date()
    inicio_mes = hoje.replace(day=1)
    
    print("\n--- PERÍODO ---")
    print(f"1. Este Mês ({inicio_mes} até {hoje})")
    print("2. Todo o Histórico (2020 até Hoje)")
    print("3. Personalizado")
    op = input("Opção: ")
    
    if op == '1':
        return inicio_mes, hoje
    elif op == '2':
        return "2020-01-01", hoje
    else:
        i = input("Data Inicio (AAAA-MM-DD): ")
        f = input("Data Fim    (AAAA-MM-DD): ")
        return i, f

def main():
    while True:
        print("\n" + "="*40)
        print("   TESTADOR DE DASHBOARD (API)")
        print("="*40)
        print("1. 📊 Cards Principais (Faturamento, Lucro...)")
        print("2. 📈 Gráfico: Evolução Diária")
        print("3. 🏆 Ranking: Produtos Mais Vendidos")
        print("4. 💳 Ranking: Formas de Pagamento")
        print("5. 🏪 Ranking: Vendedores")
        print("6. 📦 Ranking: Grupos")
        print("0. Sair")
        
        opcao = input("\nEscolha uma opção: ")
        
        if opcao == '0': break
        
        dt_ini, dt_fim = menu_datas()
        params = {"data_inicio": dt_ini, "data_fim": dt_fim}
        
        dados = None
        
        if opcao == '1':
            dados = requisitar("/reports/dashboard-cards", params)
            if dados:
                print("\n✅ RESULTADO DOS CARDS:")
                print(f"💰 Faturamento:   R$ {dados['faturamento']:,.2f}")
                print(f"🧾 Qtde Vendas:   {dados['qtde_vendas']}")
                print(f"🎫 Ticket Médio:  R$ {dados['ticket_medio']:,.2f}")
                print(f"📉 Custo (CMV):   R$ {dados['cmv']:,.2f}")
                print(f"📈 Lucro Bruto:   R$ {dados['lucro_bruto']:,.2f}")
                print(f"📊 Margem:        {dados['lucro_bruto_percent']:.2f}%")
                
        elif opcao == '2':
            dados = requisitar("/reports/chart/evolution", {**params, "tipo": "dia"})
            if dados:
                print("\n✅ GRÁFICO (Dados para o Front):")
                for ponto in dados:
                    print(f"Data: {ponto['label']} -> R$ {ponto['value']:,.2f}")

        elif opcao == '3':
            dados = requisitar("/reports/ranking/produto", {**params, "limit": 10})
            if dados:
                print("\n✅ TOP PRODUTOS:")
                print(f"{'PRODUTO':<40} | {'QTD':<10} | {'TOTAL':<15}")
                print("-" * 70)
                for item in dados:
                    print(f"{item['nome'][:40]:<40} | {item['qtd']:<10} | R$ {item['total']:,.2f}")

        elif opcao == '4':
            dados = requisitar("/reports/ranking/pagamento", params)
            if dados:
                print("\n✅ FORMAS DE PAGAMENTO:")
                for item in dados:
                    print(f"- {item['nome']}: R$ {item['total']:,.2f} ({item['qtd']} vendas)")

        elif opcao == '5':
            dados = requisitar("/reports/ranking/vendedor", params)
            if dados:
                print("\n✅ VENDEDORES:")
                for item in dados:
                    print(f"- {item['nome']}: R$ {item['total']:,.2f}")

        elif opcao == '6':
            dados = requisitar("/reports/ranking/grupo", params)
            if dados:
                print("\n✅ GRUPOS:")
                for item in dados:
                    print(f"- {item['nome']}: R$ {item['total']:,.2f}")
        
        input("\nPressione ENTER para continuar...")
        limpar_tela()

if __name__ == "__main__":
    # Verifica se tem a lib requests instalada
    try:
        import requests
        main()
    except ImportError:
        print("Erro: Você precisa instalar a biblioteca requests.")
        print("Rode: pip install requests")