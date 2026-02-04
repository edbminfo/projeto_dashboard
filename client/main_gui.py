import customtkinter as ctk
import threading
import os
import sys
import time
from datetime import datetime
from PIL import Image
import pystray
from pystray import MenuItem as item
import agente_sync

def resource_path(relative_path):
    try: base_path = sys._MEIPASS
    except Exception: base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        # 1. Tenta carregar configurações
        sucesso, msg = agente_sync.carregar_configuracoes()
        self.cnpj = agente_sync.TOKEN if agente_sync.TOKEN else "DESCONHECIDO"

        # 2. Bloqueio de Instância Única por CNPJ
        if self.verificar_lock():
            sys.exit(0)

        # 3. Configuração Visual (Dark Theme)
        ctk.set_appearance_mode("dark")
        self.title("BM Dashboard - Agente")
        self.geometry("700x520")
        self.protocol('WM_DELETE_WINDOW', self.minimizar_para_tray)

        # Cabeçalho
        ctk.CTkLabel(self, text=f"Agente Sync v{agente_sync.VERSAO}", font=("Arial", 20, "bold")).pack(pady=(15, 0))
        self.lbl_cnpj = ctk.CTkLabel(self, text=f"LOJA ATIVA: {self.cnpj}", font=("Arial", 14, "bold"), text_color="#3b8ed0")
        self.lbl_cnpj.pack(pady=(0, 15))

        # Terminal de Log
        self.log_text = ctk.CTkTextbox(self, width=660, height=300, fg_color="black", text_color="#2ecc71", font=("Consolas", 12))
        self.log_text.pack(padx=20, pady=10, fill="both", expand=True)

        # Botão Status
        self.btn_status = ctk.CTkButton(self, text="AGENTE ATIVO", fg_color="green", state="disabled", width=200)
        self.btn_status.pack(pady=20)

        # Setup do Tray
        self.setup_tray()

        # Início Automático
        if sucesso:
            self.adicionar_log(f"Configurações carregadas para o CNPJ: {self.cnpj}")
            threading.Thread(target=self.rodar_sync, daemon=True).start()
            self.after(3000, self.minimizar_para_tray)
        else:
            self.adicionar_log(f"ERRO DE CONFIGURAÇÃO: {msg}", erro=True)

    def verificar_lock(self):
        """Impede que dois agentes da mesma loja rodem ao mesmo tempo"""
        self.lock_file = os.path.join(os.path.dirname(__file__), f"lock_{self.cnpj}.tmp")
        if os.path.exists(self.lock_file):
            try: os.remove(self.lock_file)
            except: return True # Não conseguiu apagar, significa que está em uso
        open(self.lock_file, 'w').write(str(os.getpid()))
        return False

    def setup_tray(self):
        img = Image.open(resource_path("logo.ico"))
        menu = (item('Abrir Painel', self.mostrar_janela), item('Sair', self.encerrar))
        self.tray = pystray.Icon("BMSync", img, f"BM Sync - {self.cnpj}", menu)
        threading.Thread(target=self.tray.run, daemon=True).start()

    def adicionar_log(self, texto, erro=False):
        timestamp = datetime.now().strftime("[%H:%M:%S]")
        cor = " [ERRO] " if erro else " "
        self.log_text.insert("end", f"{timestamp}{cor}{texto}\n")
        self.log_text.see("end")

    def rodar_sync(self):
        agente_sync.configurar_estrutura_banco()
        while True:
            try:
                if agente_sync.executar_ciclo_sync():
                    self.adicionar_log("Dados sincronizados com sucesso.")
                time.sleep(agente_sync.DELAY_OCIOSO)
            except Exception as e:
                self.adicionar_log(f"Erro no ciclo: {e}", erro=True)
                time.sleep(10)

    def minimizar_para_tray(self): self.withdraw()
    def mostrar_janela(self): self.deiconify()
    def encerrar(self):
        if os.path.exists(self.lock_file): os.remove(self.lock_file)
        self.tray.stop()
        self.quit()

if __name__ == "__main__":
    App().mainloop()