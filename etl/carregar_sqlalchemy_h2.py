import pandas as pd
import sqlalchemy
from pathlib import Path
import sys

def conectar_h2(db_path: str = "data/h2_banco.mv.db") -> sqlalchemy.engine.Engine:
    
    """
    Retorna uma conexão SQLalchemy com o banco H2 local.
    """
    jdbc_url = f"jdbc:h2:file:{Path(db_path).resolve().parent}/h2_banco"
    return sqlalchemy.create_engine(f"h2+zxjdbc:///{jdbc_url}", echo=False)

def carregar_dados(df: pd.DataFrame, engine: sqlalchemy.engine.Engine, tabela: str = "paradas") -> None:
    """
    Realiza a carfa do DataFrame para o banco H2.
    """
    df.to_sql(tabela, con=engine, if_exists="replace", index=False)
    print(f"Carga concluída: {len(df)} registros inseridos na tabela '{tabela}'")
    
if __name__ == "__main__":
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from etl.transformar_dados import limpar_dados
    from etl.extrair_excel import ler_planilha
    
    df_original = ler_planilha()
    df_limpo = limpar_dados(df_original)
    
    engine = conectar_h2()
    carregar_dados(df_limpo, engine)