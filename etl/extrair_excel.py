from pathlib import Path
import pandas as pd
import os

def ler_planilha():
    """
    Lê a planilha de ocorrência diretamente da pasta 'data'.
    Retorna um Dataframe com os dados.
    """
    caminho_arquivo = Path(__file__).parent.parent / "data" / "Base_Paradas_Preditivas_Reativas.xlsx"
  
    if not caminho_arquivo.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_arquivo}")

    try:
        df = pd.read_excel(caminho_arquivo, sheet_name=0, engine="openpyxl")
        print(f"Planilha lida com sucesso: {caminho_arquivo.name}")
        return df
    except Exception as e:
        print(f"Erro ao ler planilha: {e}")
        raise

       

# Teste rápido
if __name__ == "__main__":
    dados = ler_planilha()
    print(f"Número de registros encontrados: {len(dados)}")
    print(dados.head())  # Exibe as primeiras linhas