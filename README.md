# CNPJ Collector API

Este é um projeto Python que utiliza a API CNPJá para coletar e consolidar dados empresariais em um banco de dados PostgreSQL.

## Descrição

O objetivo deste projeto é facilitar a obtenção e armazenamento de informações detalhadas sobre empresas brasileiras. A API CNPJá fornece acesso a uma variedade de dados, incluindo CNPJ, razão social, data de fundação, atividades principais e secundárias, entre outros.

O código Python neste repositório é responsável por realizar as seguintes tarefas:

1. Consultar a API CNPJá para obter informações sobre empresas com base em seus CNPJs.
2. Atualizar uma visualização materializada no banco de dados para obter novos CNPJs a serem coletados.
3. Consolidar os dados obtidos da API e inseri-los em uma tabela no banco de dados.

## Pré-requisitos

- Python 3.x
- PostgreSQL
- Bibliotecas Python:
    - psycopg2
    - http.client
    - json
    - time
    - datetime

## Uso

1. Clone este repositório:

    ```
    git clone https://github.com/seuusuario/api_cnpj_consult.git
    ```

2. Execute o script `db_structure.sql` para configurar as estruturas do banco de dados, adaptando para que a VIEW MATERIALIZADA utilize sua tabela de clientes.

3. Configure as variáveis de conexão com o banco de dados no arquivo `cnpj_collector.py`.

4. Execute o script Python `cnpj_consult_api.py`:

    ```
    python3 cnpj_consult_api.py
    ```

Isso iniciará a coleta de dados da API CNPJá e a inserção dos dados consolidados no banco de dados!
