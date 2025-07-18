"""
- Limpeza: remover nulos, duplicados, espaços e caracteres estranhos
- Padronização: unificar tipos de parada, impacto, equipamento, turnos etc
- Conversão de tipos: garantir que campos como Data e Duração estejam no formato certo
- Validação: detectar registros suspeitos ou inconsistentes para tratamento
"""


from pathlib import Path
import pandas as pd
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))


def padronizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza os nomes das colunas e aplica renomeações para compatibilidade com o banco.
    """
    # Remove espaços e substitui por underline
    df.columns = df.columns.str.strip().str.replace(" ", "_")

    # Renomeia para nomes internos padronizados
    df = df.rename(columns={
        "Tipo_Parada": "tipo",
        "Equipamento": "equipamentop",
        "Data_Hora": "data",
        "Duração_(min)": "duracao",  # Confirmado pelo cabeçalho
        "Causa_Principal": "causa",
        "Ação_Corretiva": "acao",
        "Prevenção_Recomendada": "prevencao",
        "Impacto_Produção": "impacto",
        "Responsável": "responsavel"
    })

    return df

def limpar_dados(df: pd.DataFrame) -> pd.DataFrame:
    df = padronizar_colunas(df)

    df = df.dropna(how="all")

    campos_obrigatorios = ["tipo", "equipamentop", "data", "duracao", "causa", 
                           "acao", "prevencao", "impacto", "responsavel"]
    
    
    for campo in campos_obrigatorios:
        if campo in df.columns:
            df[campo] = df[campo].fillna("Não informado")

    df["tipo"] = df["tipo"].str.title().str.strip()
    df["impacto"] = df["impacto"].str.upper().str.strip()

    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")

    if "duracao" in df.columns:
        df["duracao"] = pd.to_numeric(df["duracao"], errors="coerce")

    df = df.dropna(subset=["data", "duracao"])

    return df

if __name__ == "__main__":
    from etl.extrair_excel import ler_planilha
    df_original = ler_planilha()
    df_limpo = limpar_dados(df_original)
    print(f"Registro após transformação: {len(df_limpo)}")
    
    