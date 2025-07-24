import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

#from banco_utils import conectar_h2, criar_tabela, inserir_dados, listar_registros
from banco_manager import H2Manager
from etl.transformar_dados import limpar_dados
from etl.extrair_excel import ler_planilha
from utils.diagnostico_utils import diagnostico_estrutura

if __name__ == "__main__":
    df_original = ler_planilha()                 # 1. Extrai a planilha
    diagnostico_estrutura(df_original, nome="Planilha original")
    df_limpo = limpar_dados(df_original)         # 2. Limpa com ETL modular
    diagnostico_estrutura(df_limpo, nome="DataFrame tratado")

    h2 = H2Manager()                             # 3. Instancia o gerenciador de banco
    h2.conectar()                                # 4. Conecta via JDBC
    h2.criar_tabela()                            # 5. Cria estrutura no banco
    h2.inserir_dados(df_limpo)                   # 6. Carrega os dados com batch insert
    h2.listar_registros()                        # 7. Exibe registros
    h2.fechar()                                  # 8. Fecha conexão com graça
    
    """
    Usar junto com banco_utils
    conn = conectar_h2()                         # 3. Conecta no H2 via JDBC
    criar_tabela(conn)                           # 4. Cria a estrutura da tabela
    inserir_dados(df_limpo, conn)                # 5. Carrega os dados no banco
    listar_registros(conn)                       # 6. Exibe os primeiros registros
    conn.close()                                 # 7. Encerra conexão com graça
    """