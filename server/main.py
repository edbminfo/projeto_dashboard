from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from database_utils import init_master_table
# Importa os 3 roteadores
from routers import admin, sync, reports 

app = FastAPI(title="Dashboard API Multi-Tenant")

# Permite acesso de qualquer origem (CORS) - Importante para o App/Site funcionar
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup():
    init_master_table()

# Registra as rotas
app.include_router(admin.router, prefix="/api")   # Criar Cliente
app.include_router(sync.router, prefix="/api")    # Receber Dados
app.include_router(reports.router, prefix="/api") # Gerar Relatórios

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)