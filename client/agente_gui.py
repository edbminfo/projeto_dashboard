import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import configparser
import threading
import time
import os
import sys
import queue
import requests
import fdb
import decimal
import ctypes 
from datetime import datetime, date, time as dt_time
from PIL import Image # ImageDraw não é mais necessário se usarmos o .ico
import pystray

# --- CONFIGURAÇÕES DE DIRETÓRIO ---
# Função para encontrar arquivos (imagens/configs) dentro do EXE ou na pasta
def resource_path(relative_path):
    """ Retorna o caminho absoluto para o recurso, funciona para dev e para PyInstaller """
    try:
        # PyInstaller cria uma pasta temporária e armazena o caminho em _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)

# Caminho do Config (sempre externo ao EXE para poder editar)
if getattr(sys, 'frozen', False):
    app_path = os.path.dirname(sys.executable)
else:
    app_path = os.path.dirname(os.path.abspath(__file__))

config_path = os.path.join(app_path, 'config.ini')
versao_app = "v2.3 Stable"

# Filas e Eventos
log_queue = queue.Queue()
stop_event = threading.Event()
pause_event = threading.Event()

# --- LÓGICA DE LOG E BANCO ---
def log_msg(msg, erro=False):
    timestamp = datetime.now().strftime("[%H:%M:%S]")
    prefix = "[ERRO] " if erro else "[INFO] "
    try:
        log_queue.put(f"{timestamp} {prefix}{msg}")
    except: pass

def get_db_connection(config):
    try:
        return fdb.connect(
            host=config['host'], port=int(config['port']), database=config['caminho'], 
            user=config['usuario'], password=config['senha'], charset='ISO8859_1'
        )
    except Exception as e:
        log_msg(f"Falha DB: {e}", erro=True)
        return None

def sync_thread_func(config_api, config_db):
    """Core do Sincronismo com Rotina de Inicialização Restaurada"""
    
    # --- 1. ROTINA DE INICIALIZAÇÃO (Roda uma vez ao iniciar) ---
    conn = get_db_connection(config_db)
    if conn:
        try:
            cursor = conn.cursor()
            log_msg(f"--- Agente Sync Iniciado ({config_api['data_corte']}) ---")

            # Definição das tabelas
            TABELAS_SYNC = [
                {"nome": "USUARIOS", "endpoint": "/api/sync/cadastros/usuario_pdv", "sql": "ID, NOME, LOGIN"},
                {"nome": "SECAO", "endpoint": "/api/sync/cadastros/secao", "sql": "ID, SECAO AS NOME"},
                {"nome": "GRUPO", "endpoint": "/api/sync/cadastros/grupo", "sql": "ID, GRUPO AS NOME, ID_SECAO"},
                {"nome": "FAMILIA", "endpoint": "/api/sync/cadastros/familia", "sql": "ID, FAMILIA AS NOME"},
                {"nome": "VENDEDOR", "endpoint": "/api/sync/cadastros/vendedor", "sql": "ID, NOME, COMISSAO, ATIVO"},
                {"nome": "FABRICANTE", "endpoint": "/api/sync/cadastros/fabricante", "sql": "ID, FABRICANTE AS NOME"},
                {"nome": "PESSOA", "endpoint": "/api/sync/cadastros/cliente", "sql": "ID, NOME, CNPJ_CPF, CIDADE, ATIVO"},
                {"nome": "FORMAPAG", "endpoint": "/api/sync/cadastros/formapag", "sql": "ID, FORMAPAG AS NOME, TIPO"},
                {"nome": "PRODUTO", "endpoint": "/api/sync/cadastros/produto", "sql": "ID, NOME, PRECO_VENDA, CUSTO_TOTAL, ID_GRUPO, ID_FABRICANTE, ID_FORNECEDOR, ID_FAMILIA, ATIVO"},
                {"nome": "SAIDA", "endpoint": "/api/sync/saida", "sql": "ID, ID_FILIAL, DATA, HORA, TOTAL, ID_CLIENTE, TERMINAL, USUARIO AS ID_USUARIO, ELIMINADO, NORMAL, NUMERO, SERIE, TIPOSAIDA, TIPO, CHAVENFE"},
                {"nome": "SAIDA_PRODUTO", "endpoint": "/api/sync/saida_produto", "sql": "ID, ID_SAIDA, ID_PRODUTO, ID_VENDEDOR, QUANT, TOTAL"},
                {"nome": "SAIDA_FORMAPAG", "endpoint": "/api/sync/saida_formapag", "sql": "ID_SAIDA, ID_FORMAPAG, VALOR"},
            ]

            # 1.1 Configura colunas SYNK_DASH_PEND
            for t in TABELAS_SYNC:
                tbl = t['nome']
                try:
                    cursor.execute(f"SELECT FIRST 1 SYNK_DASH_PEND FROM {tbl}")
                except:
                    log_msg(f">> Configurando infraestrutura em {tbl}...")
                    conn.commit() # commit transação anterior falha
                    try:
                        cursor.execute(f"ALTER TABLE {tbl} ADD SYNK_DASH_PEND CHAR(1) DEFAULT 'S'")
                        conn.commit()
                        # Tenta criar trigger de update (opcional, pode falhar se já existir)
                        cursor.execute(f"""
                            CREATE OR ALTER TRIGGER TG_SYNC_{tbl[:20]} FOR {tbl}
                            ACTIVE BEFORE INSERT OR UPDATE POSITION 99
                            AS BEGIN
                                IF (NEW.SYNK_DASH_PEND IS NULL OR NEW.SYNK_DASH_PEND = OLD.SYNK_DASH_PEND OR INSERTING) THEN
                                    NEW.SYNK_DASH_PEND = 'S';
                            END
                        """)
                        conn.commit()
                    except Exception as e:
                        log_msg(f"Erro ao criar campo em {tbl}: {e}", erro=True)

            # 1.2 Manutenção de Integridade (Limpeza de dados antigos)
            log_msg(f">> Manutenção de integridade (Corte: {config_api['data_corte']})...")
            
            # Marca como 'N' (processado) tudo que for velho
            cursor.execute(f"UPDATE SAIDA SET SYNK_DASH_PEND = 'N' WHERE DATA < '{config_api['data_corte']}' AND SYNK_DASH_PEND = 'S'")
            
            # Limpa filhos órfãos ou antigos (Produto e FormaPag)
            sql_limpeza = f"""
                UPDATE {{tabela}} SET SYNK_DASH_PEND = 'N' 
                WHERE SYNK_DASH_PEND = 'S' 
                AND NOT EXISTS (
                    SELECT 1 FROM SAIDA 
                    WHERE SAIDA.ID = {{tabela}}.ID_SAIDA 
                    AND SAIDA.DATA >= '{config_api['data_corte']}'
                )
            """
            cursor.execute(sql_limpeza.format(tabela="SAIDA_PRODUTO"))
            cursor.execute(sql_limpeza.format(tabela="SAIDA_FORMAPAG"))
            
            # Garante que nulos virem 'S'
            for t in TABELAS_SYNC:
                cursor.execute(f"UPDATE {t['nome']} SET SYNK_DASH_PEND = 'S' WHERE SYNK_DASH_PEND IS NULL")
            
            conn.commit()
            log_msg(">> Infraestrutura verificada com sucesso.")
            
        except Exception as e:
            log_msg(f"Erro na Inicialização: {e}", erro=True)
        finally:
            conn.close()
    else:
        log_msg("Não foi possível conectar para inicializar.", erro=True)
        return

    # --- 2. LOOP DE SINCRONISMO (While True) ---
    
    # Helpers
    def limpar_valor(val):
        if val is None: return None
        if isinstance(val, bytes): return val.hex()
        if isinstance(val, decimal.Decimal): return float(val)
        if isinstance(val, (datetime, date, dt_time)): return val.isoformat()
        return str(val).strip()

    def row_to_dict(row, col_names, db_key):
        data = {}
        for i, col in enumerate(col_names):
            key = col.lower().strip()
            data[key] = limpar_valor(row[i])
        data['id_original'] = data.get('id', limpar_valor(db_key))
        if 'id' in data: del data['id']
        data['cnpj_loja'] = config_api['cnpj']
        return data

    while not stop_event.is_set():
        if pause_event.is_set():
            time.sleep(1)
            continue

        encontrou_algo = False
        conn = get_db_connection(config_db)
        
        if conn:
            cursor = conn.cursor()
            try:
                # Loop de Envio
                for t in TABELAS_SYNC:
                    if stop_event.is_set(): break
                    
                    tbl = t['nome']
                    sql = f"SELECT FIRST {config_api['lote']} RDB$DB_KEY, {t['sql']}, SYNK_DASH_PEND FROM {tbl} WHERE SYNK_DASH_PEND = 'S'"
                    
                    # Filtros de Data
                    if tbl == 'SAIDA':
                        sql += f" AND DATA >= '{config_api['data_corte']}'"
                    elif tbl in ['SAIDA_PRODUTO', 'SAIDA_FORMAPAG']:
                        sql += f" AND EXISTS (SELECT 1 FROM SAIDA WHERE SAIDA.ID = {tbl}.ID_SAIDA AND SAIDA.DATA >= '{config_api['data_corte']}')"

                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    
                    if rows:
                        encontrou_algo = True
                        cols = [d[0] for d in cursor.description][1:-1]
                        payload = [row_to_dict(r[1:-1], cols, r[0]) for r in rows]
                        db_keys = [r[0] for r in rows]
                        
                        try:
                            headers = {"Authorization": f"Bearer {config_api['token']}", "X-CNPJ-Loja": config_api['cnpj']}
                            r = requests.post(f"{config_api['url']}{t['endpoint']}", json=payload, headers=headers, timeout=30)
                            
                            if r.status_code == 200:
                                cursor.executemany(f"UPDATE {tbl} SET SYNK_DASH_PEND = 'N' WHERE RDB$DB_KEY = ?", [(k,) for k in db_keys])
                                conn.commit()
                                log_msg(f"[ENVIO] {len(payload)} regs em {tbl}")
                            else:
                                log_msg(f"Erro API {tbl}: {r.status_code}", erro=True)
                        except Exception as req_err:
                            log_msg(f"Erro Conexão: {req_err}", erro=True)
                        time.sleep(0.5)
            except Exception as e:
                log_msg(f"Erro Ciclo: {e}", erro=True)
            finally:
                conn.close()
        
        if not encontrou_algo:
            time.sleep(5)
        else:
            time.sleep(1)

# --- INTERFACE GRÁFICA ---
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Agente Sync")
        self.geometry("600x620")
        self.resizable(False, False)
        
        # --- APLICAÇÃO DO ÍCONE NA JANELA ---
        try:
            self.icon_path = resource_path("logo.ico")
            self.iconbitmap(self.icon_path)
        except Exception as e:
            # Se não achar o icone, segue sem ele para não travar
            print(f"Icone não encontrado: {e}")
            self.icon_path = None
        
        # Variáveis
        self.var_cnpj = tk.StringVar()
        self.var_token = tk.StringVar()
        self.var_db_tipo = tk.StringVar(value="FIREBIRD")
        self.var_db_host = tk.StringVar(value="localhost")
        self.var_db_port = tk.StringVar(value="3050")
        self.var_db_path = tk.StringVar()
        self.var_db_user = tk.StringVar(value="SYSDBA")
        self.var_db_pass = tk.StringVar()
        
        self.thread_sync = None
        self.tray_icon = None
        self.is_minimized = False
        self.app_mutex = None 

        self.setup_ui()
        self.load_config()
        
        # 1. VERIFICAÇÃO DE LOCK (Singleton)
        if not self.check_instance_lock():
            sys.exit(0)
            
        self.check_queue() 
        self.protocol("WM_DELETE_WINDOW", self.on_close_window)

        # 2. INICIAR NA BANDEJA
        self.start_in_tray()

    def check_instance_lock(self):
        cnpj = self.var_cnpj.get().strip()
        if not cnpj: return True
        try:
            kernel32 = ctypes.windll.kernel32
            mutex_name = f"Global\\AgenteSync_{cnpj}"
            self.app_mutex = kernel32.CreateMutexW(None, False, mutex_name)
            if kernel32.GetLastError() == 183:
                messagebox.showerror("Erro", f"O Agente Sync já está rodando para o CNPJ {cnpj}.\nVerifique a bandeja do sistema.")
                return False
            return True
        except: return True

    def setup_ui(self):
        style = ttk.Style()
        style.theme_use('clam')
        
        # Header
        header = tk.Frame(self, bg="#005b9f", height=60)
        header.pack(fill="x", side="top")
        header.pack_propagate(False)
        tk.Label(header, text="Sincronizador de Dados - BM Dashboard", bg="#005b9f", fg="white", 
                 font=("Segoe UI", 14, "bold")).pack(side="left", padx=20, pady=15)

        # Body
        main = tk.Frame(self, padx=10, pady=10)
        main.pack(fill="both", expand=True)

        # ID
        lf_id = ttk.LabelFrame(main, text="Identificação", padding=10)
        lf_id.pack(fill="x", pady=5)
        self.mk_entry(lf_id, "CNPJ:", self.var_cnpj, 0, 25)
        self.mk_entry(lf_id, "Token:", self.var_token, 1, 50, "*")

        # DB
        lf_db = ttk.LabelFrame(main, text="Banco de Dados", padding=10)
        lf_db.pack(fill="x", pady=5)
        
        tk.Label(lf_db, text="Tipo:").grid(row=0, column=0, sticky="e", padx=5)
        ttk.Combobox(lf_db, textvariable=self.var_db_tipo, values=["FIREBIRD"], width=15).grid(row=0, column=1, sticky="w", padx=5)
        
        tk.Label(lf_db, text="Host/Porta:").grid(row=1, column=0, sticky="e", padx=5)
        f_h = tk.Frame(lf_db)
        f_h.grid(row=1, column=1, sticky="w")
        ttk.Entry(f_h, textvariable=self.var_db_host, width=20).pack(side="left", padx=5)
        ttk.Entry(f_h, textvariable=self.var_db_port, width=8).pack(side="left")
        
        self.mk_entry(lf_db, "Caminho DB:", self.var_db_path, 2, 50)
        
        tk.Label(lf_db, text="User/Pass:").grid(row=3, column=0, sticky="e", padx=5)
        f_u = tk.Frame(lf_db)
        f_u.grid(row=3, column=1, sticky="w")
        ttk.Entry(f_u, textvariable=self.var_db_user, width=15).pack(side="left", padx=5)
        ttk.Entry(f_u, textvariable=self.var_db_pass, width=15, show="*").pack(side="left")

        # Botoes
        btns = tk.Frame(main, pady=10)
        btns.pack(fill="x")
        tk.Button(btns, text="💾 Salvar", command=self.save_config, height=2, width=12).pack(side="left", padx=5)
        self.btn_toggle = tk.Button(btns, text="⏹ PARAR", bg="#dc3545", fg="white", font=("Bold"), height=2, width=12, command=self.toggle_sync)
        self.btn_toggle.pack(side="left", padx=20)
        tk.Button(btns, text="🔽 Ocultar", command=self.minimize_to_tray, height=2, width=12).pack(side="right", padx=5)

        # Log
        self.log_area = scrolledtext.ScrolledText(main, height=12, state='disabled', font=("Consolas", 9))
        self.log_area.pack(fill="both", expand=True, pady=(10,0))
        self.log_area.tag_config('erro', foreground='red')

    def mk_entry(self, p, lbl, var, r, w, s=None):
        tk.Label(p, text=lbl).grid(row=r, column=0, sticky="e", padx=5, pady=2)
        ttk.Entry(p, textvariable=var, width=w, show=s).grid(row=r, column=1, sticky="w", padx=5, pady=2)

    def check_queue(self):
        try:
            while True:
                msg = log_queue.get_nowait()
                self.log_area.config(state='normal')
                self.log_area.insert(tk.END, msg + "\n", 'erro' if '[ERRO]' in msg else '')
                self.log_area.see(tk.END)
                self.log_area.config(state='disabled')
        except queue.Empty: pass
        finally: self.after(100, self.check_queue)

    def load_config(self):
        cfg = configparser.ConfigParser()
        cfg.read(config_path)
        try:
            self.var_cnpj.set(cfg.get('ID', 'cnpj', fallback=''))
            self.var_token.set(cfg.get('API', 'token_loja', fallback=''))
            if cfg.has_section('DATABASE'):
                self.var_db_host.set(cfg.get('DATABASE', 'host', fallback='localhost'))
                self.var_db_port.set(cfg.get('DATABASE', 'port', fallback='3050'))
                self.var_db_path.set(cfg.get('DATABASE', 'caminho', fallback=''))
                self.var_db_user.set(cfg.get('DATABASE', 'usuario', fallback='SYSDBA'))
                self.var_db_pass.set(cfg.get('DATABASE', 'senha', fallback=''))
            
            if self.var_cnpj.get() and self.var_db_path.get():
                self.start_sync_thread()
        except: pass

    def save_config(self):
        cfg = configparser.ConfigParser()
        cfg['ID'] = {'cnpj': self.var_cnpj.get()}
        cfg['API'] = {'token_loja': self.var_token.get(), 'url_base': 'https://api-dash.bmhelp.click'}
        cfg['DATABASE'] = {
            'tipo': self.var_db_tipo.get(), 'host': self.var_db_host.get(), 'port': self.var_db_port.get(),
            'caminho': self.var_db_path.get(), 'usuario': self.var_db_user.get(), 'senha': self.var_db_pass.get()
        }
        cfg['CONFIG'] = {'data_corte': '2026-01-01', 'tamanho_lote': '50'}
        with open(config_path, 'w') as f: cfg.write(f)
        messagebox.showinfo("Sucesso", "Configurações salvas!")

    def start_sync_thread(self):
        if self.thread_sync and self.thread_sync.is_alive(): return
        stop_event.clear()
        pause_event.clear()
        cfg_api = {'cnpj': self.var_cnpj.get(), 'token': self.var_token.get(), 'url': 'https://api-dash.bmhelp.click', 'lote': 50, 'data_corte': '2026-01-01'}
        cfg_db = {'host': self.var_db_host.get(), 'port': self.var_db_port.get(), 'caminho': self.var_db_path.get(), 'usuario': self.var_db_user.get(), 'senha': self.var_db_pass.get()}
        
        self.thread_sync = threading.Thread(target=sync_thread_func, args=(cfg_api, cfg_db), daemon=True)
        self.thread_sync.start()
        self.btn_toggle.config(text="⏹ PARAR", bg="#dc3545")

    def toggle_sync(self):
        if pause_event.is_set():
            pause_event.clear()
            self.btn_toggle.config(text="⏹ PARAR", bg="#dc3545")
            log_msg("RETOMADO.")
        else:
            pause_event.set()
            self.btn_toggle.config(text="▶ INICIAR", bg="#28a745")
            log_msg("PAUSADO.")

    # --- TRAY E START MINIMIZADO ---
    def start_in_tray(self):
        """Inicia minimizado"""
        self.withdraw() 
        self.is_minimized = True
        threading.Thread(target=self._run_tray_icon, daemon=True).start()

    def create_tray_image(self):
        # Tenta carregar o logo.ico, se falhar, cria o quadrado azul (fallback)
        try:
            return Image.open(resource_path("logo.ico"))
        except:
            w, h = 64, 64
            image = Image.new('RGBA', (w, h), (0, 0, 0, 0)) 
            from PIL import ImageDraw
            d = ImageDraw.Draw(image)
            d.rectangle((4, 4, 60, 60), fill="#005b9f", outline="white", width=2)
            d.ellipse((20, 20, 44, 44), fill="white")
            return image

    def minimize_to_tray(self):
        self.withdraw() 
        self.is_minimized = True
        if self.tray_icon is None:
            threading.Thread(target=self._run_tray_icon, daemon=True).start()

    def _run_tray_icon(self):
        image = self.create_tray_image()
        menu = pystray.Menu(
            pystray.MenuItem("Abrir Dashboard", self.restore_from_tray, default=True),
            pystray.MenuItem("Sair Totalmente", self.quit_app)
        )
        self.tray_icon = pystray.Icon("AgenteSync", image, f"Agente Sync - {self.var_cnpj.get()}", menu)
        self.tray_icon.run()

    def restore_from_tray(self, icon=None, item=None):
        if self.tray_icon:
            self.tray_icon.stop() 
            self.tray_icon = None
        self.after(0, self._show_window_safe)

    def _show_window_safe(self):
        self.deiconify() 
        self.is_minimized = False
        self.lift()
        self.attributes('-topmost',True)
        self.after_idle(self.attributes,'-topmost',False)

    def on_close_window(self):
        if messagebox.askyesno("Sair", "Deseja minimizar para a bandeja?\n(Se clicar em Não, o programa será fechado)"):
            self.minimize_to_tray()
        else:
            self.quit_app()

    def quit_app(self, icon=None, item=None):
        stop_event.set()
        if self.tray_icon:
            self.tray_icon.stop()
        self.destroy()
        sys.exit(0)

if __name__ == "__main__":
    app = App()
    app.mainloop()