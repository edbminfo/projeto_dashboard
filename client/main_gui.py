import customtkinter as ctk
import threading
import time
import sys
from datetime import datetime
# Importamos as funções do seu script original
from agente_sync import executar_ciclo_sync, verificar_delecoes, configurar_estrutura_banco, VERSAO, DATA_CORTE, TOKEN

class AgenteSyncGUI(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title(f"Agente Sync Dashboard - v{VERSAO}")
        self.geometry("600x700")
        self.is_running = False

        # --- Layout da Interface (Baseado na sua foto) ---
        self.setup_ui()

        # --- Inicialização Automática ---
        self.adicionar_log(f"Configuração detectada. Iniciando automaticamente...")
        self.iniciar_sincronismo()

    def setup_ui(self):
        # Frame Identificação
        self.frame_api = ctk.CTkFrame(self)
        self.frame_api.pack(fill="x", padx=20, pady=10)
        ctk.CTkLabel(self.frame_api, text="Identificação (API)", font=("Arial", 12, "bold")).pack(anchor="w", padx=10)
        self.criar_campo(self.frame_api, "CNPJ:", "11222333000198")
        self.criar_campo(self.frame_api, "Token:", "********************", show="*")

        # Frame Banco de Dados
        self.frame_db = ctk.CTkFrame(self)
        self.frame_db.pack(fill="x", padx=20, pady=10)
        ctk.CTkLabel(self.frame_db, text="Configuração Firebird / DB", font=("Arial", 12, "bold")).pack(anchor="w", padx=10)
        self.criar_campo(self.frame_db, "Tipo:", "FIREBIRD")
        self.criar_campo(self.frame_db, "Host/IP:", "localhost")
        self.criar_campo(self.frame_db, "Caminho DB:", r"C:\datacash\DADOS\BM.FDB")

        # Botões
        self.frame_btn = ctk.CTkFrame(self, fg_color="transparent")
        self.frame_btn.pack(fill="x", padx=20, pady=10)
        
        self.btn_acao = ctk.CTkButton(self.frame_btn, text="PARAR", fg_color="#d35400", command=self.toggle_sync)
        self.btn_acao.pack(side="left", expand=True, padx=5)
        
        ctk.CTkButton(self.frame_btn, text="Ocultar", command=self.withdraw).pack(side="left", expand=True, padx=5)

        # Terminal de Log (Preto com letras verdes como na foto)
        self.txt_log = ctk.CTkTextbox(self, height=250, fg_color="black", text_color="#2ecc71", font=("Consolas", 12))
        self.txt_log.pack(fill="both", expand=True, padx=20, pady=10)

    def criar_campo(self, master, label, valor, show=None):
        f = ctk.CTkFrame(master, fg_color="transparent")
        f.pack(fill="x", padx=10, pady=2)
        ctk.CTkLabel(f, text=label, width=100, anchor="w").pack(side="left")
        e = ctk.CTkEntry(f, show=show)
        e.insert(0, valor)
        e.pack(side="left", fill="x", expand=True)

    def adicionar_log(self, mensagem):
        timestamp = datetime.now().strftime("[%H:%M:%S]")
        self.txt_log.insert("end", f"{timestamp} {mensagem}\n")
        self.txt_log.see("end")

    def iniciar_sincronismo(self):
        if not self.is_running:
            self.is_running = True
            self.btn_acao.configure(text="PARAR", fg_color="#d35400")
            # Executa a manutenção inicial e o loop em Background
            threading.Thread(target=self.trabalho_sync, daemon=True).start()

    def toggle_sync(self):
        if self.is_running:
            self.is_running = False
            self.btn_acao.configure(text="INICIAR", fg_color="#27ae60")
            self.adicionar_log("Serviço de sincronização PARADO.")
        else:
            self.iniciar_sincronismo()
            self.adicionar_log("Serviço de sincronização REINICIADO.")

    def trabalho_sync(self):
        """Este método substitui o 'while True' do seu agente_sync.py original"""
        configurar_estrutura_banco() #
        self.adicionar_log("Serviço de sincronização INICIADO.")

        while self.is_running:
            try:
                fez_algo = False
                # Aqui chamamos as funções do seu arquivo original
                if executar_ciclo_sync(): fez_algo = True #
                if verificar_delecoes(): fez_algo = True #

                if not fez_algo:
                    time.sleep(30) # Delay ocioso
                else:
                    self.adicionar_log("Dados enviados com sucesso! ✅")
                    time.sleep(1) # Delay entre lotes
            except Exception as e:
                self.adicionar_log(f"ERRO: {str(e)}")
                time.sleep(10)

if __name__ == "__main__":
    app = AgenteSyncGUI()
    app.mainloop()