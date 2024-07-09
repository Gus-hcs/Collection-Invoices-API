import pyodbc
import requests
import time
from datetime import datetime, timedelta

server = '###'
database = '###'
username = '###'
password = '###'

connection = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = connection.cursor()

api_keys_and_tables = [
    {'api_key': '###', 'table': '###'},
    {'api_key': '###', 'table': '###'},
    {'api_key': '###', 'table': '###'},
    {'api_key': '###', 'table': '###'},
    {'api_key': '###', 'table': '###'}
]


def make_request_with_retry(url, delay=1, max_retries=5):
    for _ in range(max_retries):
        response = requests.get(url)
        if response.status_code == 429:
            print("Limite de requisições atingido. Aguardando para tentar novamente.")
            time.sleep(delay)
        elif response.status_code == 200:
            return response
    return None

for item in api_keys_and_tables:
    api_key = item['api_key']
    table_name = item['table']
    page = 1
    tipo = "S"
    data_hoje = datetime.today().date()
    data_inicio = (data_hoje - timedelta(days=7)).strftime("%d/%m/%Y")
    data_final = data_hoje.strftime("%d/%m/%Y")
    data_exclusao_sql = (data_hoje - timedelta(days=7)).strftime("%Y-%m-%d")

    while True:
        url = f'https://bling.com.br/Api/v2/notasfiscais/page={page}/json?apikey={api_key}&filters=dataEmissao[{data_inicio} TO {data_final}];tipo[{tipo}]'
        response = make_request_with_retry(url)

        if response is None:
            print("Não foi possível obter resposta após várias tentativas. Encerrando o processo para API key: ", api_key)
            break

        if response.status_code == 200:
            data = response.json()
            if 'retorno' in data and 'notasfiscais' in data['retorno']:
                notas_fiscais = data['retorno']['notasfiscais']
                if not notas_fiscais:
                    print("Não há mais registros nesta página. Passando para a próxima página.")
                    page += 1
                    continue

                for nota in notas_fiscais:
                    notafiscal = nota.get('notafiscal', {})
                    numero = notafiscal.get('numero')
                    xml = notafiscal.get('xml')
                    cnpj = notafiscal.get('cnpj')
                    linkdanfe = notafiscal.get('linkDanfe')
                    loja = notafiscal.get('loja')
                    situacao = notafiscal.get('situacao')
                    tipo = notafiscal.get('tipo')
                    vendedor = notafiscal.get('vendedor')
                    valornota = notafiscal.get('valorNota')
                    chaveacesso = notafiscal.get('chaveAcesso')
                    contato = notafiscal.get('contato')
                    serie = notafiscal.get('serie')
                    dataemissao = notafiscal.get('dataEmissao')
                    cliente = notafiscal.get('cliente', {})
                    cliente_cnpj = cliente.get('cnpj')
                    cliente_nome = cliente.get('nome')
                    cursor.execute(f"""
                        INSERT INTO {table_name} (
                            numero, xml, cnpj, linkdanfe, loja, situacao, tipo, vendedor, valornota, chaveacesso,
                            contato, serie, dataemissao, cliente_cnpj, cliente_nome
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        numero, xml, cnpj, linkdanfe, loja, situacao, tipo, vendedor, valornota, chaveacesso,
                        contato, serie, dataemissao, cliente_cnpj, cliente_nome
                    ))
                    connection.commit()

                page += 1
                time.sleep(0.6)
            else:
                print("Não foram encontradas notas fiscais nesta página para API key: ", api_key)
                break
        else:
            print(f"Erro na solicitação HTTP: Código {response.status_code}")

connection.close()
