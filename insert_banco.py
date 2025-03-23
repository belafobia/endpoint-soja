from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from main import PrecoFuturoSoja, Base

DATABASE_URL = "sqlite:///./soja.db"
motor = create_engine(DATABASE_URL)
SessaoLocal = sessionmaker(autocommit=False, autoflush=False, bind=motor)

Base.metadata.create_all(bind=motor)

def popular_tabela():
    sessao = SessaoLocal()
    try:
        meses = [
            "JAN24", "FEV24", "MAR24", "ABR24", "MAI24", "JUN24",
            "JUL24", "AGO24", "SET24", "OUT24", "NOV24", "DEZ24"
        ]
        precos = [400.00 + i * 5 for i in range(len(meses))]

        for i, (mes, preco) in enumerate(zip(meses, precos)):
            registro = PrecoFuturoSoja(
                id=str(i + 1),
                mes_contrato=mes,
                preco=preco,
                criado_em=datetime.utcnow()
            )
            sessao.add(registro)

        sessao.commit()
        print("Dados inseridos com sucesso!")
    except Exception as e:
        sessao.rollback()
        print(f"Erro ao inserir dados: {e}")
    finally:
        sessao.close()

if __name__ == "__main__":
    popular_tabela()