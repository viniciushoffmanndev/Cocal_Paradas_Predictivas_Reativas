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

def padronizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza os nomes das colunas e aplica renomeações para compatibilidade com o banco.
    Remove espaços, acentos e caracteres especiais.
    """

    df.columns = ( # Normaliza nomes: tira espaços extras, acentos, pontuação e converte para minúsculas
        df.columns
        .str.strip()                                # Remove espaços nas bordas
        .str.lower()                                # Minúsculas
        .str.replace(r"[^\w\s]", "", regex=True)    # Remove pontuação
        .str.replace(" ", "_")                      # Espaço vira underscore
    )
    
    df.columns = [ # Remove acentuação
        unicodedata.normalize("NFKD", col).encode("ASCII", "ignore").decode("utf-8")
        for col in df.columns
    ]
    
    print("→ Colunas padronizadas:", df.columns.tolist())  # Visualiza resultado da padronização
    
    df = df.rename(columns={ # Renomeia conforme padrão do banco
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

    return df

def limpar_dados(df: pd.DataFrame) -> pd.DataFrame: # Normaliza estrutura do DataFrame para persistência no banco
    
    """
    Realiza transformação nos dados brutos extraídos da planilha:
    - Padroniza nomes de colunas
    - Trata nulos e espaços extras
    - Formata datas e durações
    - Filtra colunas relevantes para persistência
    """
    
    df = padronizar_colunas(df)
    df = df.dropna(how="all")
    
    campos_obrigatorios = [
        "tipo", "equipamento", "data", "duracao", "causa", 
        "acao", "prevencao", "impacto", "responsavel"
        ]
    
    faltanto = [col for col in campos_obrigatorios if col not in df.columns] # Verificação de presença das colunas
    
    if faltanto:
        raise ValueError(f"Colunas obrigatórias ausentes: {faltanto}")
    
    for campo in campos_obrigatorios: # Continuação da limpeza ...
        df[campo] = df[campo].fillna("Não informado")
                  
    df["tipo"] = df["tipo"].str.title().str.strip() # Capitaliza os valores da coluna 'tipo' para manter consistência visual (ex: "Parada Mecânica")
    df["impacto"] = df["impacto"].str.upper().str.strip() # Converte 'impacto' para caixa alta para facilitar agrupamentos e filtros posteriores
    
    for coluna in df.select_dtypes(include="object").columns:  # Remove espaços extras nas colunas de texto, exceto 'tipo' e 'impacto' que já foram tratados acima
        if coluna not in ["tipo", "impacto"]:
            df[coluna] = df[coluna].str.strip()

        if "data" in df.columns: # Converte a coluna 'data' para datetime no formato ISO 8601 (padrão universal para bancos)
            df["data"] = pd.to_datetime(df["data"], errors="coerce").fillna(pd.Timestamp("1900-01-01"))

        if "duracao" in df.columns:
            df["duracao"] = pd.to_numeric(df["duracao"], errors="coerce").fillna(0).astype(int)
        
        os.makedirs("auditorias", exist_ok=True) # Auditoria de registros rejeitados (por data ou duração inválida)
        registro_hora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        arquivo = f"auditorias/registros_rejeitados_{registro_hora}.xlsx"

        registros_rejeitados = df[df["data"].isna() | df["duracao"].isna()]
        if not registros_rejeitados.empty:
            registros_rejeitados.to_excel(arquivo, index=False)
            print(f"{len(registros_rejeitados)} registros rejeitados salvos em '{arquivo}'")
            
        colunas_desejadas = [ # Garantir que o DataFrame tenha somente as colunas esperadas
        "tipo", "equipamento", "data", "duracao", "causa",
        "acao", "prevencao", "impacto", "responsavel"
        ]

        # Finaliza estrutura
        df = df[colunas_desejadas]

        return df

if __name__ == "__main__": # Ponto de entrada do script: leitura da planilha e aplicação da limpeza, somente se o arquivo for executado diretamente
    from etl.extrair_excel import ler_planilha
    df_original = ler_planilha()
    df_limpo = limpar_dados(df_original)
    print(f"Registro após transformação: {len(df_limpo)}")
    
    