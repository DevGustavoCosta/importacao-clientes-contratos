import pandas as pd
import psycopg2
from psycopg2 import Error as Psycopg2Error
from tqdm import tqdm
import re

# Dados da conexão
DB_HOST = "localhost"
DB_NAME = "clientes_contratos"
DB_USER = "postgres"
DB_PASS = "157010"
DB_PORT = "5432"

# Dicionário de conversão de estado para sigla
estado_para_sigla = {
    "Acre": "AC",
    "Alagoas": "AL",
    "Amapá": "AP",
    "Amazonas": "AM",
    "Bahia": "BA",
    "Ceará": "CE",
    "Distrito Federal": "DF",
    "Espírito Santo": "ES",
    "Goiás": "GO",
    "Maranhão": "MA",
    "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS",
    "Minas Gerais": "MG",
    "Pará": "PA",
    "Paraíba": "PB",
    "Paraná": "PR",
    "Pernambuco": "PE",
    "Piauí": "PI",
    "Rio de Janeiro": "RJ",
    "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS",
    "Rondônia": "RO",
    "Roraima": "RR",
    "Santa Catarina": "SC",
    "São Paulo": "SP",
    "Sergipe": "SE",
    "Tocantins": "TO"
}

#Função para conectar com banco
def conectar_banco():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        return conn
    except Psycopg2Error as e:
        print(f"Erro ao conectar ao banco: {e}")
        exit(1)

#Função para ler arquivo xlsx
def ler_arquivo_excel(caminho):
    try:
        return pd.read_excel(caminho, dtype={'Telefone': str, 'Celular': str})
    except Exception as e:
        print(f"Erro ao ler Excel: {e}")
        exit(1)

#Função para bucas cliente no banco
def buscar_cliente(cursor, cpf_cnpj):
    cursor.execute("SELECT id FROM tbl_clientes WHERE cpf_cnpj = %s", (cpf_cnpj,))
    return cursor.fetchone()

#Função para inserir cliente no banco
def inserir_cliente(cursor, dados):
    cursor.execute("""
        INSERT INTO tbl_clientes (nome_razao_social, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, dados)
    return cursor.fetchone()[0]

#Função para inserir os contatos do cliente
def inserir_contatos(cursor, cliente_id, contatos):
    comandos = []

    for contato, tipo in contatos:
        if contato:
            # Só insere se não existir
            cursor.execute("""
                SELECT id FROM tbl_cliente_contatos 
                WHERE cliente_id = %s AND tipo_contato_id = %s
            """, (cliente_id, tipo))
            if not cursor.fetchone():
                comandos.append((cliente_id, tipo, str(contato)))
    if comandos:
        cursor.executemany("""
            INSERT INTO tbl_cliente_contatos (cliente_id, tipo_contato_id, contato)
            VALUES (%s, %s, %s)
            """, comandos)

#Função para buscar os planos
def buscar_plano(cursor, plano_desc):
    cursor.execute("SELECT id FROM tbl_planos WHERE descricao = %s", (plano_desc,))
    plano = cursor.fetchone()
    if plano:
        return plano[0]
    else:
        raise ValueError(f"Plano '{plano_desc}' não encontrado.")

#Função para buscar status
def buscar_status(cursor, status_nome):
    cursor.execute("SELECT id FROM tbl_status_contrato WHERE status = %s", (status_nome,))
    status = cursor.fetchone()
    return status[0] if status else None

#Função para inserir contrato
def inserir_contrato(cursor, cliente_id, plano_id, vencimento, isento, endereco, status_id):
    cursor.execute("""
        INSERT INTO tbl_cliente_contratos (
            cliente_id, plano_id, dia_vencimento, isento,
            endereco_logradouro, endereco_numero, endereco_bairro, endereco_cidade,
            endereco_complemento, endereco_cep, endereco_uf, status_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        cliente_id, plano_id, vencimento, isento,
        endereco['logradouro'], endereco['numero'], endereco['bairro'], endereco['cidade'],
        endereco['complemento'], endereco['cep'], endereco['uf'], status_id
    ))


# #Função para tratar CPF ou CNPJ
# def tratar_cpf_cnpj(cpf_cnpj):
#     valido_cpf_cnpj = re.sub(r'\D', '', str(cpf_cnpj))

#     if len(valido_cpf_cnpj) == 11:
#         return f"{valido_cpf_cnpj[:3]}.{valido_cpf_cnpj[3:6]}.{valido_cpf_cnpj[6:9]}-{valido_cpf_cnpj[9:]}"
#     elif len(valido_cpf_cnpj) == 14:
#         return f"{valido_cpf_cnpj[:2]}.{valido_cpf_cnpj[2:5]}.{valido_cpf_cnpj[5:8]}/{valido_cpf_cnpj[8:12]}-{valido_cpf_cnpj[12:]}"
#     else:
#         return None  


# Função para tratar CPF ou CNPJ
def tratar_cpf_cnpj(cpf_cnpj):

    valido_cpf_cnpj = re.sub(r'\D', '', str(cpf_cnpj))
    
    if len(valido_cpf_cnpj) in [11, 14]:
        return valido_cpf_cnpj
    else:
        return None

# Função para limpar o cep e padronizar
def tratar_cep(cep):

    cep_tratado = re.sub(r'\D', '', str(cep))
    
    if len(cep_tratado) == 8:
        
        return cep_tratado  #f"{cep_tratado[:5]}-{cep_tratado[5:]}"
    else:
        return None
    
#Função para tratar telefone
def tratar_telefone(telefone):
    if telefone and isinstance(telefone, str):
        return telefone.split('.')[0]
    elif isinstance(telefone, (int, float)):
        return str(int(telefone))
    return telefone

#Função de processamento das informações da planilha
def processar_registro(cursor, row):
    Nome_RazSocial  = row.iloc[0] if pd.notna(row.iloc[0]) else None
    Nome_Fantasia   = row.iloc[1] if pd.notna(row.iloc[1]) else None
    Cpf_Cnpj        = tratar_cpf_cnpj(row.iloc[2]) if pd.notna(row.iloc[2]) else None
    DataNasc        = row.iloc[3] if pd.notna(row.iloc[3]) else None
    Dt_CadastroCli  = row.iloc[4] if pd.notna(row.iloc[4]) else None
    Celular         = tratar_telefone(row.iloc[5]) if pd.notna(row.iloc[5]) else None
    Telefone        = tratar_telefone(row.iloc[6]) if pd.notna(row.iloc[6]) else None
    Email           = row.iloc[7] if pd.notna(row.iloc[7]) else None
    Endereco        = row.iloc[8] if pd.notna(row.iloc[8]) else None
    Num_Enderco     = row.iloc[9] if pd.notna(row.iloc[9]) else None
    Comple_Endereco = row.iloc[10] if pd.notna(row.iloc[10]) else None
    Bairro          = row.iloc[11] if pd.notna(row.iloc[11]) else None
    Cep             = tratar_cep(row.iloc[12]) if pd.notna(row.iloc[12]) else None
    Cidade          = row.iloc[13] if pd.notna(row.iloc[13]) else None
    Uf_nome         = row.iloc[14] if pd.notna(row.iloc[14]) else None
    Uf              = estado_para_sigla.get(Uf_nome, None) if Uf_nome else None
    Plano           = row.iloc[15] if pd.notna(row.iloc[15]) else None
    Vencimento      = row.iloc[17] if pd.notna(row.iloc[17]) else None
    Status          = row.iloc[18] if pd.notna(row.iloc[18]) else None
    Isento          = row.iloc[19] if pd.notna(row.iloc[19]) else False

    if not Cpf_Cnpj:
        raise ValueError("CPF/CNPJ ausente ou inválido.")

    cliente = buscar_cliente(cursor, Cpf_Cnpj)
    if cliente:
        cliente_id = cliente[0]
    else:
        cliente_id = inserir_cliente(cursor, (
            Nome_RazSocial, Nome_Fantasia, Cpf_Cnpj, DataNasc, Dt_CadastroCli
        ))

    contatos = [(Celular, 1), (Telefone, 2), (Email, 3)]
    inserir_contatos(cursor, cliente_id, contatos)

    if Plano:
        plano_id = buscar_plano(cursor, Plano)
        status_id = buscar_status(cursor, Status)
        endereco = {
            'logradouro': Endereco,
            'numero': Num_Enderco,
            'bairro': Bairro,
            'cidade': Cidade,
            'complemento': Comple_Endereco,
            'cep': Cep,
            'uf': Uf
        }
        inserir_contrato(cursor, cliente_id, plano_id, int(Vencimento) if Vencimento else None, bool(Isento), endereco, status_id)

#Função principal
def main():
    conn = conectar_banco()
    cursor = conn.cursor()

    df = ler_arquivo_excel('C:/Users/gusta/OneDrive/Desktop/Arquivo/dados_importacao.xlsx')

    importados = 0
    nao_importados = []

    print("\nIniciando importação...\n")
    for index, row in tqdm(df.iterrows(), total=len(df), desc="Importando registros", unit="registro", colour="green"):
        try:
            processar_registro(cursor, row)
            conn.commit()
            importados += 1
        except (ValueError, Psycopg2Error) as e:
            conn.rollback()
            nao_importados.append((index + 2, str(e)))  # +2 pois a primeira linha é cabeçalho

    cursor.close()
    conn.close()

    print("\n=== RESUMO DA IMPORTAÇÃO ===")
    print(f"Total de registros importados com sucesso: {importados}")
    print(f"Total de registros não importados: {len(nao_importados)}\n")

    if nao_importados:
        with open("erros_importacao.txt", "w", encoding="utf-8") as f:
            f.write("Registros não importados:\n")
            for linha, motivo in nao_importados:
                texto = f"Linha {linha}: {motivo}\n"
                print(texto.strip())
                f.write(texto)
        print("\nErro na importação de alguns registros. Verifique o arquivo 'erros_importacao.txt'.")
    else:
        print("\nTodos os registros foram importados com sucesso.")

if __name__ == "__main__":
    main()
