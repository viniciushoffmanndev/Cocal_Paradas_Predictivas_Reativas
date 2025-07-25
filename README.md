
# Do Excel ao Banco H2: Um Pipeline ETL Modular com Python e Java

![ETL Pipeline](https://img.shields.io/badge/ETL-Python%20%2B%20Java-green)
![Modularização](https://img.shields.io/badge/Arquitetura-Modular-blue)
![Banco H2](https://img.shields.io/badge/Banco-H2-yellow)
![Status](https://img.shields.io/badge/Status-Em%20Desenvolvimento-orange)

## 📌 Descrição

Este projeto apresenta um pipeline ETL (Extract, Transform, Load) desenvolvido com **Python** para extração e transformação de dados de planilhas Excel, e integração com o **banco de dados H2** utilizando **Java (JDBC)**. A solução foi desenhada de forma modular para garantir **escalabilidade, confiabilidade e reusabilidade**.

## 🗂️ Arquitetura do Projeto

O pipeline foi dividido em módulos independentes, com responsabilidades bem definidas:

| Módulo                | Responsabilidade                                                                 |
|-----------------------|-----------------------------------------------------------------------------------|
| `extrair_excel.py`    | Leitura da planilha Excel (.xlsx) com validação de existência                    |
| `transformar_dados.py`| Limpeza, padronização, validação e auditoria dos dados                           |
| `banco_manager.py`    | Persistência no banco H2 com JDBC (via `jaydebeapi`)                             |
| `carregar_h2.py`      | Orquestração da pipeline de ponta a ponta                                        |
| `diagnostico_utils.py`| Diagnóstico estrutural dos DataFrames                                            |

## 🧪 Pipeline ETL

### 🔹 1. Extração

O módulo `extrair_excel.py` usa `pandas` e `openpyxl` para ler a planilha `.xlsx` da pasta `/data`.

```python
df = pd.read_excel("data/entrada.xlsx", engine="openpyxl")
```

### 🔹 2. Transformação

No `transformar_dados.py`, são aplicadas:

- Padronização de colunas (sem acentos, espaços, símbolos)
- Tratamento de nulos e normalização de textos
- Conversão de datas e durações
- Geração de logs de auditoria para registros rejeitados

### 🔹 3. Carga no H2

O `banco_manager.py` conecta ao banco H2 via **JDBC + JayDeBeApi**:

- Criação da tabela com schema dinâmico
- Conversão de datas para `YYYY-MM-DD HH:MM:SS`
- Inserção em lote (`executemany`)
- Fechamento seguro da conexão

### 🔹 4. Execução

O `carregar_h2.py` executa toda a pipeline:

```bash
python carregar_h2.py
```

Saída esperada no terminal:

```bash
✔ Dados extraídos com sucesso.
✔ Dados transformados e validados.
✔ Tabela criada no H2.
✔ Dados inseridos com sucesso.
✔ Registros listados.
```

## 🧰 Tecnologias e Bibliotecas

- **Python 3.10+**
  - `pandas`
  - `openpyxl`
  - `jaydebeapi`
  - `unicodedata`
  - `datetime`
  - `os` / `pathlib`

- **Java**
  - Driver JDBC do H2 Database

## 🧠 Lições Aprendidas

- A padronização de colunas é fundamental para interoperabilidade.
- A auditoria de rejeições melhora a confiabilidade e rastreabilidade.
- A modularização facilita a manutenção e testes.
- A integração entre Python e Java amplia o poder do pipeline.
- Cada erro e exceção foi tratado como oportunidade de aprendizado técnico.

## 📁 Estrutura de Diretórios

```
etlpipeline/
├── data/
│   └── entrada.xlsx
├── extrair_excel.py
├── transformar_dados.py
├── banco_manager.py
├── carregar_h2.py
├── diagnostico_utils.py
├── auditoria/
│   └── rejeitados.xlsx
└── README.md
```

## ▶️ Como Executar

### Pré-requisitos

- Python 3.10+
- Java JDK 8+
- Banco H2 (`h2.jar`) disponível no diretório `/lib`

### Passos

1. Clone o repositório:

```bash
git clone https://github.com/seu-usuario/seu-repositorio.git
cd seu-repositorio
```

2. Instale as dependências:

```bash
pip install -r requirements.txt
```

3. Execute o pipeline:

```bash
python carregar_h2.py
```

## 🌐 Recursos e Documentações

- [Python.org](https://www.python.org)
- [Pandas](https://pandas.pydata.org)
- [openpyxl](https://openpyxl.readthedocs.io)
- [JayDeBeApi](https://pypi.org/project/JayDeBeApi)
- [H2 Database](http://www.h2database.com/html/main.html)

## 📌 Autor

**Vinicius Hoffmann**  
[GitHub](https://github.com/viniciushoffmanndev)
[LinkedIn](www.linkedin.com/in/viniciushoffmanndev)

## 📄 Licença

Este projeto está licenciado sob a [MIT License](LICENSE).
