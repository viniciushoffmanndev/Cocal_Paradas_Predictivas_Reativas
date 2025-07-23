import jaydebeapi
import os
import pandas as pd

DRIVER_PATH = "./drivers/h2-2.3.232.jar"
DB_PATH = "./data/h2_banco"
DEFAULT_TABELA = "paradas"
USER = os.getenv("H2_USER", "")
PASSWORD = os.getenv("H2_PASSWORD", "")

def conectar_h2():
    """Estabelece conexão com banco H2 via JDBC."""
    conn = jaydebeapi.connect(
        "org.h2.Driver",
        f"jdbc:h2:file:{DB_PATH}",
        [USER, PASSWORD],
        os.path.abspath(DRIVER_PATH)
    )
    print("Conexão com H2 estabelecida")
    return conn

def criar_tabela(conn, tabela=DEFAULT_TABELA):
    """Cria a tabela no banco com estrutura compatível com o DataFrame limpo."""
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {tabela}")
    cursor.execute(f"""
        CREATE TABLE {tabela} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            tipo VARCHAR,
            equipamentop VARCHAR,
            data TIMESTAMP,
            duracao INT,
            causa VARCHAR,
            acao VARCHAR,
            prevencao VARCHAR,
            impacto VARCHAR,
            responsavel VARCHAR
        )
    """)
    cursor.close()
    print(f"Tabela '{tabela}' criada com sucesso")

def inserir_dados(df: pd.DataFrame, conn, tabela=DEFAULT_TABELA):
    """Insere os dados do DataFrame na tabela H2."""
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute(
            f"INSERT INTO {tabela} (tipo, equipamento, data, duracao, causa, acao, prevencao, impacto, responsavel) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tuple(row.values)
        )
    conn.commit()
    cursor.close()
    print(f"{len(df)} registros inseridos na tabela '{tabela}'")

def listar_registros(conn, tabela=DEFAULT_TABELA, limite=10):
    """Exibe os primeiros registros da tabela para conferência."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {tabela} LIMIT {limite}")
    rows = cursor.fetchall()
    cursor.close()

    print(f"Registros da tabela '{tabela}':")
    for row in rows:
        print(row)