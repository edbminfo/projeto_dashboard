from fastapi import FastAPI
from database_utils import init_master_table
from routers import admin, sync

app = FastAPI(title="Dashboard API Multi-Tenant")

# Inicializa a tabela mestra ao ligar
@app.on_event("startup")
def startup():
    init_master_table()

# Inclui as rotas
app.include_router(admin.router, prefix="/api")
app.include_router(sync.router, prefix="/api")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)