import pandas as pd

def diagnostico_estrutura(df: pd.DataFrame, nome: str = "DataFrame") -> None:
    """
    Exibe diagnóstico rápido da estrutura de um DataFrame:
    - Total de registros
    - Nomes das colunas
    - Tipos de dados
    - Amostra de registros
    - Quantidade de nulos por coluna
    """
    print(f"\nDiagnóstico: {nome}")
    print("→ Total de registros:", len(df))
    print("→ Colunas:", df.columns.tolist())
    print("\nTipos de dados:")
    print(df.dtypes)
    print("\nAmostra:")
    print(df.head(5))
    print("\nNulos por coluna:")
    print(df.isna().sum())
    print("-" * 50)