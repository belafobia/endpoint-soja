from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse
from decimal import Decimal
from typing_extensions import Annotated
from pydantic import BaseModel, Field
from typing import List
from datetime import datetime, timezone
from sqlalchemy import create_engine, Column, String, DateTime, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

DATABASE_URL = "sqlite:///./soja.db"
motor = create_engine(DATABASE_URL)
SessaoLocal = sessionmaker(autocommit=False, autoflush=False, bind=motor)
Base = declarative_base()

class PrecoFuturoSoja(Base):
    __tablename__ = "precos_futuros_soja"
    id = Column(String, primary_key=True, index=True)
    mes_contrato = Column(String, nullable=False)
    preco = Column(Numeric(10, 2), nullable=False)
    criado_em = Column(DateTime, default=lambda: datetime.now(timezone.utc))

Base.metadata.create_all(bind=motor)

class RequisicaoPrecoFixo(BaseModel):
    base: Annotated[Decimal, Field(ge=Decimal(-50), le=Decimal(50))]
    meses_contratos: List[str]

class ResultadoPrecoFixo(BaseModel):
    mes_contrato: str
    preco_cbot: float
    base: float
    preco_fixo: float

class RespostaPrecoFixo(BaseModel):
    resultados: List[ResultadoPrecoFixo]

app = FastAPI()

def obter_sessao():
    sessao = SessaoLocal()
    try:
        yield sessao
    finally:
        sessao.close()

def buscar_preco_futuro(sessao: Session, mes_contrato: str):
    mes_contrato = mes_contrato.strip().upper()
    registro = sessao.query(PrecoFuturoSoja).filter(PrecoFuturoSoja.mes_contrato == mes_contrato).order_by(PrecoFuturoSoja.criado_em.desc()).first()
    return registro

@app.post("/api/preco_fixo", response_model=RespostaPrecoFixo)
def calcular_preco_fixo(requisicao: RequisicaoPrecoFixo, sessao: Session = Depends(obter_sessao)):
    resultados = []
    fator_conversao = 1.10231

    for mes_contrato in requisicao.meses_contratos:
        registro_preco = buscar_preco_futuro(sessao, mes_contrato)
        if not registro_preco:
            raise HTTPException(status_code=404, detail=f"Mês de contrato {mes_contrato} não encontrado")

        preco_cbot = float(registro_preco.preco)
        base = float(requisicao.base)
        preco_fixo = (preco_cbot + base) * fator_conversao

        resultados.append({
            "mes_contrato": mes_contrato,
            "preco_cbot": preco_cbot,
            "base": base,
            "preco_fixo": round(preco_fixo, 2)
        })

    return {"resultados": resultados}

@app.exception_handler(HTTPException)
async def tratar_erro_http(requisicao, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"erro": exc.detail}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)