import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import configparser
import threading
import os
import sys
import time
from PIL import Image, ImageDraw
import pystray

# Importe suas funções lógicas existentes
from agente_sync import configurar_estrutura_banco, executar_ciclo_sync, verificar_delecoes

class AgenteSyncGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Agente Sync Dashboard")
        self.root.geometry("600x750")
        self.config_path = 'config.ini'
        self.running = False
        self.icon = None
        
        self.setup_ui()
        self.carregar_configuracoes()
        
        # Lógica de Auto-Início
        self.verificar_auto_start()

    def setup_ui(self):
        # Cabeçalho
        header = tk.Frame(self.root, bg="#0056b3")
        header.pack(fill="x")
        tk.Label(header, text="🔄 Sincronizador de Dados - BM Dashboard", 
                 fg="white", bg="#0056b3", font=("Arial", 12, "bold")).pack(pady=10)

        # Seção Identificação
        sec_id = tk.LabelFrame(self.root, text="Identificação (API)", padx=10, pady=5)
        sec_id.pack(fill="x", padx=10, pady=5)
        
        self.ent_cnpj = self.add_field(sec_id, "CNPJ:", 0)
        self.ent_token = self.add_field(sec_id, "Token:", 1, show="*")

        # Seção Banco de Dados (Mais Completa)
        sec_db = tk.LabelFrame(self.root, text="Configuração Firebird / DB", padx=10, pady=5)
        sec_db.pack(fill="x", padx=10, pady=5)

        tk.Label(sec_db, text="Tipo:").grid(row=0, column=0, sticky="w")
        self.cb_tipo = ttk.Combobox(sec_db, values=["FIREBIRD", "POSTGRESQL"])
        self.cb_tipo.grid(row=0, column=1, sticky="w", padx=5, pady=2)

        self.ent_host = self.add_field(sec_db, "Host/IP:", 1)
        self.ent_port = self.add_field(sec_db, "Porta:", 2)
        self.ent_path = self.add_field(sec_db, "Caminho DB (.fdb):", 3)
        self.ent_user = self.add_field(sec_db, "Usuário DB:", 4)
        self.ent_pass = self.add_field(sec_db, "Senha DB:", 5, show="*")

        # Botões de Ação
        btn_frame = tk.Frame(self.root)
        btn_frame.pack(pady=10)
        
        tk.Button(btn_frame, text="💾 Salvar Config", command=self.salvar_configuracoes, width=15).pack(side="left", padx=5)
        self.btn_control = tk.Button(btn_frame, text="▶️ INICIAR", bg="#28a745", fg="white", command=self.toggle_sync, width=15)
        self.btn_control.pack(side="left", padx=5)
        tk.Button(btn_frame, text="⏬ Ocultar", command=self.minimizar_para_tray, width=15).pack(side="left", padx=5)

        # Console de Logs
        self.txt_log = scrolledtext.ScrolledText(self.root, height=12, bg="#1e1e1e", fg="#d4d4d4", font=("Consolas", 9))
        self.txt_log.pack(fill="both", padx=10, pady=10)

    def add_field(self, parent, label, row, show=None):
        tk.Label(parent, text=label).grid(row=row, column=0, sticky="w")
        entry = tk.Entry(parent, width=55, show=show)
        entry.grid(row=row, column=1, padx=5, pady=2)
        return entry

    def log(self, msg):
        timestamp = time.strftime("%H:%M:%S")
        self.txt_log.insert(tk.END, f"[{timestamp}] {msg}\n")
        self.txt_log.see(tk.END)

    def carregar_configuracoes(self):
        config = configparser.ConfigParser()
        if os.path.exists(self.config_path):
            config.read(self.config_path)
            self.ent_cnpj.insert(0, config.get('ID', 'cnpj', fallback=''))
            self.ent_token.insert(0, config.get('API', 'token_loja', fallback=''))
            self.cb_tipo.set(config.get('DB', 'type', fallback='FIREBIRD'))
            self.ent_host.insert(0, config.get('DB', 'host', fallback='localhost'))
            self.ent_port.insert(0, config.get('DB', 'port', fallback='3050'))
            self.ent_path.insert(0, config.get('DB', 'path', fallback=''))
            self.ent_user.insert(0, config.get('DB', 'user', fallback='SYSDBA'))
            self.ent_pass.insert(0, config.get('DB', 'pass', fallback='masterkey'))

    def salvar_configuracoes(self):
        config = configparser.ConfigParser()
        config['ID'] = {'cnpj': self.ent_cnpj.get()}
        config['API'] = {'token_loja': self.ent_token.get(), 'url_base': 'https://api-dash.bmhelp.click'}
        config['DB'] = {
            'type': self.cb_tipo.get(),
            'host': self.ent_host.get(),
            'port': self.ent_port.get(),
            'path': self.ent_path.get(),
            'user': self.ent_user.get(),
            'pass': self.ent_pass.get()
        }
        with open(self.config_path, 'w') as f:
            config.write(f)
        self.log("Configurações salvas com sucesso.")

    def verificar_auto_start(self):
        path = self.ent_path.get()
        if path and os.path.exists(path):
            self.log("Configuração detectada. Iniciando automaticamente...")
            self.root.after(1000, self.toggle_sync) # Inicia o sync
            self.root.after(2000, self.minimizar_para_tray) # Minimiza
        else:
            self.log("Aguardando configuração manual do banco de dados.")

    def loop_sincronismo(self):
        while self.running:
            try:
                # Aqui chama as funções do seu agente_sync.py
                executar_ciclo_sync()
                verificar_delecoes()
                time.sleep(10) # Intervalo entre ciclos
            except Exception as e:
                self.log(f"Erro no ciclo: {e}")
                time.sleep(30)

    def toggle_sync(self):
        if not self.running:
            self.running = True
            self.btn_control.config(text="🛑 PARAR", bg="#dc3545")
            self.log("Serviço de sincronização INICIADO.")
            threading.Thread(target=self.loop_sincronismo, daemon=True).start()
        else:
            self.running = False
            self.btn_control.config(text="▶️ INICIAR", bg="#28a745")
            self.log("Serviço de sincronização PARADO.")

    def minimizar_para_tray(self):
        self.root.withdraw()
        # Cria ícone simples para o Tray
        image = Image.new('RGB', (64, 64), color=(0, 86, 179))
        d = ImageDraw.Draw(image)
        d.text((10, 20), "SYNC", fill=(255, 255, 255))
        
        menu = (pystray.MenuItem('Abrir', self.restaurar_janela),
                pystray.MenuItem('Sair', self.encerrar_total))
        self.icon = pystray.Icon("agente", image, "BM Sync Agente", menu)
        threading.Thread(target=self.icon.run, daemon=True).start()

    def restaurar_janela(self):
        if self.icon:
            self.icon.stop()
        self.root.deiconify()

    def encerrar_total(self):
        self.running = False
        if self.icon:
            self.icon.stop()
        self.root.destroy()
        sys.exit(0)

if __name__ == "__main__":
    root = tk.Tk()
    app = AgenteSyncGUI(root)
    root.mainloop()