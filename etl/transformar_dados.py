"""
- Limpeza: remover nulos, duplicados, espaços e caracteres estranhos
- Padronização: unificar tipos de parada, impacto, equipamento, turnos etc
- Conversão de tipos: garantir que campos como Data e Duração estejam no formato certo
- Validação: detectar registros suspeitos ou inconsistentes para tratamento
"""

from datetime import datetime
from pathlib import Path
import pandas as pd
import sys
import unicodedata
import os

sys.path.append(str(Path(__file__).resolve().parent.parent))

def limpar_espacos_texto(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove espaços extras e espaços internos duplicados de todas as colunas de texto.
    """
    for coluna in df.select_dtypes(include="object").columns:
        df[coluna] = df[coluna].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    return df

def limpar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza transformação nos dados brutos extraídos da planilha:
    - Padroniza nomes de colunas
    - Trata nulos e espaços extras
    - Formata datas e durações
    - Filtra colunas relevantes para persistência
    """
    import unicodedata

    # Padroniza nomes de colunas
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w\s]", "", regex=True)
        .str.replace(" ", "_")
    )
    df.columns = [
        unicodedata.normalize("NFKD", col).encode("ASCII", "ignore").decode("utf-8")
        for col in df.columns
    ]

    df = df.rename(columns={
        "tipo_parada": "tipo",
        "equipamento": "equipamento",
        "data_hora": "data",
        "duracao_min": "duracao",
        "causa_principal": "causa",
        "acao_corretiva": "acao",
        "prevencao_recomendada": "prevencao",
        "impacto_producao": "impacto",
        "responsavel": "responsavel"
    }, errors="ignore")

    df = df.dropna(how="all")

    campos_obrigatorios = [
        "tipo", "equipamento", "data", "duracao", "causa",
        "acao", "prevencao", "impacto", "responsavel"
    ]

    faltando = [col for col in campos_obrigatorios if col not in df.columns]
    if faltando:
        raise ValueError(f"Colunas obrigatórias ausentes: {faltando}")

    for campo in campos_obrigatorios:
        df[campo] = df[campo].fillna("Não informado")

    # Limpeza de espaços e normalização de texto
    df = limpar_espacos_texto(df)

    # Conversão de tipos
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce").fillna(pd.Timestamp("1900-01-01"))

    if "duracao" in df.columns:
        df["duracao"] = pd.to_numeric(df["duracao"], errors="coerce").fillna(0).astype(int)

    # Auditoria de registros rejeitados
    os.makedirs("auditorias", exist_ok=True)
    registro_hora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    arquivo = f"auditorias/registros_rejeitados_{registro_hora}.xlsx"
    registros_rejeitados = df[df["data"].isna() | df["duracao"].isna()]
    if not registros_rejeitados.empty:
        registros_rejeitados.to_excel(arquivo, index=False)
        print(f"{len(registros_rejeitados)} registros rejeitados salvos em '{arquivo}'")

    colunas_desejadas = [
        "tipo", "equipamento", "data", "duracao", "causa",
        "acao", "prevencao", "impacto", "responsavel"
    ]
    df = df[colunas_desejadas]

    return df

if __name__ == "__main__": # Ponto de entrada do script: leitura da planilha e aplicação da limpeza, somente se o arquivo for executado diretamente
    from etl.extrair_excel import ler_planilha
    df_original = ler_planilha()
    df_limpo = limpar_dados(df_original)
    print(f"Registro após transformação: {len(df_limpo)}")
    
    