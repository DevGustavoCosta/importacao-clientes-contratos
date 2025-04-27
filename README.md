# Importação de Dados para Clientes e Contratos

Este projeto realiza a importação de dados de clientes e contratos de um arquivo Excel para um banco de dados PostgreSQL.

## Como Usar

1. **Pré-requisitos**:
   - Python 3.x
   - PostgreSQL
   - Dependências listadas no `requirements.txt`

2. **Instalação**:
   - Clone o repositório:
     ```bash
     git clone https://github.com/DevGustavoCosta/importacao-clientes-contratos.git
     cd importacao-clientes-contratos
     ```

   - Crie e ative um ambiente virtual:
     ```bash
     python -m venv venv
     source venv/bin/activate  # Para Linux/Mac
     venv\Scripts\activate     # Para Windows
     ```
     * Caso precise fazer alterar a política de execução de scripts
     ```bash
     Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
     ```

   - Instale as dependências:
     ```bash
     pip install -r requirements.txt
     ```

3. **Configuração do Banco de Dados**:
   - Altere os dados de conexão com o banco de dados na variável `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASS` no código Python.

4. **Executando o Código**:
   - Execute o script principal:
     ```bash
     python src/main.py
     ```

## Dependências

As dependências podem ser instaladas com o seguinte comando:

```bash
pip install -r requirements.txt
