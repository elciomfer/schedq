import asyncio
import datetime
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from schedq import Schedq

logger = logging.getLogger("schedq")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(handler)

schedq = Schedq()

@asynccontextmanager
async def lifespan(app: FastAPI):
    schedq.start()
    app.state.resource = schedq
    yield
    schedq.stop()
    app.state.resource = None

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"flows": list(app.state.resource.taskmap.keys())}


# Contador para simular instabilidade e testar o Circuit Breaker / Retries do Step
contador_falhas = 0

@schedq.step(name="Passo Instável", maxretries=2, retrydelay=1)
async def step_instavel(prefixo: str):
    global contador_falhas
    contador_falhas += 1
    
    if contador_falhas <= 2:
        raise ConnectionError(f"Falha de conexão simulada ({contador_falhas})")
    
    contador_falhas = 0
    return f"{prefixo} - Sucesso na execução!"


# O Flow principal gerenciado pelo Min-Heap otimizado da v0.0.3
@schedq.flow(
    interval=datetime.timedelta(seconds=5), 
    name="Pipeline de Validação", 
    maxinstances=1,
    args=("Ambiente Web Ativo",) # Testando a nova passagem de argumentos dinâmicos!
)
async def pipeline_principal(tid: str, eid: str, name: str, mensagem: str):
    agora = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{agora}] Início do Flow [{name}] - Mensagem injetada: {mensagem}")
    
    # Chama o step resiliente
    resultado = await step_instavel(mensagem)
    
    print(f"[{agora}] 🌐 {resultado}")