import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from banco_utils import conectar_h2, criar_tabela, inserir_dados, listar_registros
from etl.transformar_dados import limpar_dados
from etl.extrair_excel import ler_planilha

if __name__ == "__main__":
    df_original = ler_planilha()                 # 1. Extrai a planilha
    df_limpo = limpar_dados(df_original)         # 2. Limpa com ETL modular

    conn = conectar_h2()                         # 3. Conecta no H2 via JDBC
    criar_tabela(conn)                           # 4. Cria a estrutura da tabela
    inserir_dados(df_limpo, conn)                # 5. Carrega os dados no banco
    listar_registros(conn)                       # 6. Exibe os primeiros registros
    conn.close()                                 # 7. Encerra conexão com graça