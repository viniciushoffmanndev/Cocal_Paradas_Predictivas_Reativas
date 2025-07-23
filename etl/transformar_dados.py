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


def padronizar_colunas(df: pd.DataFrame) -> pd.DataFrame: # Padroniza os nomes de colunas e aplica renomeações compatíveis com o banco
    """
    Padroniza os nomes das colunas e aplica renomeações para compatibilidade com o banco.
    """
    
    df.columns = df.columns.str.strip().str.replace(" ", "_") # Remove espaços e substitui por underline

    
    df = df.rename(columns={ # Renomeia para nomes internos padronizados
        "Tipo_Parada": "tipo",
        "Equipamento": "equipamentop",
        "Data_Hora": "data",
        "Duração_(min)": "duracao",
        "Causa_Principal": "causa",
        "Ação_Corretiva": "acao",
        "Prevenção_Recomendada": "prevencao",
        "Impacto_Produção": "impacto",
        "Responsável": "responsavel"
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
        "acao", "prevencao", "impacto", "responsavel"]
    
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

    if "data" in df.columns:  # Converte a coluna 'data' para datetime no formato ISO 8601 (padrão universal para bancos)
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df["data"] = df["data"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    
    if "duracao" in df.columns: # Trata a coluna 'duração', convertendo valores textuais ou com vírgula para inteiros seguros
        df["duracao"] = (
            df["duracao"]
            .astype(str) # garantir que virou string
            .str.replace(",", ".", regex=False) # se tiver virgula decimal
            .str.extract(r"(\d+)")[0] # extrai apenas número válido
        )
        
        df = df.dropna(subset=["duracao"]) # limpa nulo antes de converter
        df ["duracao"] = df["duracao"].astype(int) # faz conversão segura        
        
        df = df.dropna(subset=["data", "duracao"])
        
        colunas_desejadas = [ # Garantir que o DataFrame tenha somente as colunas esperadas
        "tipo", "equipamento", "data", "duracao", "causa",
        "acao", "prevencao", "impacto", "responsavel"
        ]
        
        df = df[colunas_desejadas]
        
        # Confirmar a estrutura final
        print("→ Colunas finais:", df.columns.tolist())
        print("→ Registros:", len(df))
        
        print("→ Amostra da coluna 'data' original:")
        print(df["data"].head(5))
        print("→ Tipo detectado:", df["data"].dtype)
        print("→ Cabeçalho original da planilha:")
        print(df.columns.tolist())
        print("→ Shape do DataFrame:", df.shape)
        print(df.head(5))


        return df

if __name__ == "__main__": # Ponto de entrada do script: leitura da planilha e aplicação da limpeza, somente se o arquivo for executado diretamente
    from etl.extrair_excel import ler_planilha
    df_original = ler_planilha()
    df_limpo = limpar_dados(df_original)
    print(f"Registro após transformação: {len(df_limpo)}")
    
    