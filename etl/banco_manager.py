import jaydebeapi
import os
import pandas as pd

class H2Manager:
    """
    Classe para gerenciar operações de banco de dados H2 via JDBC.
    Permite conexão, criação de tabela, inserção em lote, listagem e encerramento.
    """

    def __init__(self, db_path="./data/h2_banco", driver_path="./drivers/h2-2.3.232.jar",
                 tabela="paradas", user_env="H2_USER", password_env="H2_PASSWORD"):
        # Caminhos e configurações do banco
        self.db_path = db_path
        self.driver_path = driver_path
        self.tabela = tabela
        self.user = os.getenv(user_env, "")
        self.password = os.getenv(password_env, "")
        self.conn = None  # Será definido com conectar()

    def conectar(self):
        """Estabelece conexão com o banco H2 via JDBC."""
        self.conn = jaydebeapi.connect(
            "org.h2.Driver",
            f"jdbc:h2:file:{self.db_path}",
            [self.user, self.password],
            os.path.abspath(self.driver_path)
        )
        print("Conexão com H2 estabelecida")

    def criar_tabela(self):
        """Cria estrutura da tabela no banco H2."""
        cursor = self.conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {self.tabela}")
        cursor.execute(f"""
            CREATE TABLE {self.tabela} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                tipo VARCHAR,
                equipamento VARCHAR,
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
        print(f"Tabela '{self.tabela}' criada com sucesso")

    def inserir_dados(self, df: pd.DataFrame):
        """Realiza inserção dos dados em lote via executemany."""
        cursor = self.conn.cursor()

        query = f"""
            INSERT INTO {self.tabela} 
            (tipo, equipamento, data, duracao, causa, acao, prevencao, impacto, responsavel)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        COLUNAS_BANCO = [
            "tipo", "equipamento", "data", "duracao", "causa", 
            "acao", "prevencao", "impacto", "responsavel"
        ]

        """
        Isso transforma os valores datetime em strings seguras como '2025-07-01 08:20:00', 
        compatíveis com a coluna TIMESTAMP no H2 e evita que o driver tente lidar com objetos Timestamp.
        df["data"] = df["data"].dt.strftime("%Y-%m-%d %H:%M:%S")
        Com isso, o executemany vai funcionar sem reclamar dos tipos, e os registros serão
        persistidos corretamente.
        """
        # Converte coluna "data" para string no formato aceito pelo JDBC
        df["data"] = df["data"].dt.strftime("%Y-%m-%d %H:%M:%S")
        registros = [tuple(row) for row in df[COLUNAS_BANCO].values]  # Transforma o DataFrame em tuplas

        try:
            cursor.executemany(query, registros)
            self.conn.commit()
            print(f"{len(df)} registros inseridos na tabela '{self.tabela}'")
        except Exception as e:
            self.conn.rollback()
            print(f"Erro ao inserir dados: {e}")
        finally:
            cursor.close()

    def listar_registros(self, limite=10):
        """Exibe os primeiros registros da tabela H2."""
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {self.tabela} LIMIT {limite}")
        rows = cursor.fetchall()
        cursor.close()

        print(f"Registros da tabela '{self.tabela}':")
        for row in rows:
            print(row)

    def fechar(self):
        """Encerra conexão com o banco."""
        if self.conn:
            self.conn.close()
            print("Conexão encerrada")