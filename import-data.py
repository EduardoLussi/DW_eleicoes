import mysql.connector
import pandas as pd
import numpy as np
from sys import stdout
import time
from datetime import timedelta

BUFFER_SIZE = 5000  # Tamanho do buffer de registros a serem inseridos ao mesmo tempo
SAVE_POINT = 20000  # Commit a cada SAVE_POINT registros lidos

START_POINT = 0     # Linha do dataframe para iniciar a leitura
END_POINT = None    # Linha da dataframe para finalizar a leitura

# ----- Conexão com MySQL
try:
    conn = mysql.connector.connect(host='localhost',
                                   database='DW',
                                   user='user',
                                   password='password',
                                   port=6033)
except mysql.connector.Error as err:
    print(err)

print('Connected to database!')

# Criação do cursor
cursor = conn.cursor(buffered=True)


# ----- Leitura dados votos
data = pd.read_csv('./data/2022-SC.csv', sep=';', encoding='cp1252', # Número de linhas a serem lidas para testes
                   usecols=['ANO_ELEICAO', 'NR_TURNO', 'SG_UF', 'NM_MUNICIPIO', 'NR_ZONA', 'NR_SECAO',
                            'DS_CARGO_PERGUNTA', 'NR_VOTAVEL', 'SG_PARTIDO', 'NM_VOTAVEL', 
                            'QT_VOTOS', 'QT_COMPARECIMENTO', 'DS_TIPO_VOTAVEL'],
                   dtype={'ANO_ELEICAO': np.int16, 'NR_TURNO': np.int8, 'NR_VOTAVEL': str, 
                          'QT_VOTOS': np.int16, 'QT_COMPARECIMENTO': np.int32})
data['DS_CARGO_PERGUNTA'] = data['DS_CARGO_PERGUNTA'].str.upper() # Cargo CAIXA ALTA
data['NM_MUNICIPIO'] = data['NM_MUNICIPIO'].str.replace("'", " ")      # Remover apóstrofo
print(f'Arquivo-fonte:\n{data}\n')

# Obtém ano, turno e uf da eleição
ano, turno, uf = data['ANO_ELEICAO'][0], data['NR_TURNO'][0], data['SG_UF'][0]

# Obtém agrupamento por municípios
data_candidatos_municipios = data.groupby(['NM_MUNICIPIO', 'DS_CARGO_PERGUNTA', 'DS_TIPO_VOTAVEL',
                                           'SG_PARTIDO', 'NM_VOTAVEL', 'NR_VOTAVEL'], 
                                          as_index=False)['QT_VOTOS'].sum()

# Obtém dados do comparecimento
data_municipios = data[['NM_MUNICIPIO', 'NR_ZONA', 'NR_SECAO', 'QT_COMPARECIMENTO']].drop_duplicates()
data_municipios = data_municipios.groupby(['NM_MUNICIPIO'])['QT_COMPARECIMENTO'].sum()


# ----- Leitura candidatos
candidatos = pd.read_csv('./data/2022-candidatos.csv', sep=';', encoding='cp1252',
                          usecols=['DS_CARGO', 'NM_URNA_CANDIDATO', 'NR_CPF_CANDIDATO',
                                   'SG_PARTIDO', 'NR_CANDIDATO', 'SG_UF'],
                          dtype={'NR_CPF_CANDIDATO': str, 'NR_CANDIDATO': str})
candidados = candidatos.loc[(candidatos['SG_UF'] == uf) | (candidatos['SG_UF'] == 'BR')]
print(f'Candidatos:\n{candidatos}\n')


# ----- Insere eleição
cursor.execute(f'INSERT INTO eleicao (ano) \
                 SELECT * FROM (SELECT {ano} AS ano) AS tmp \
                 WHERE NOT EXISTS (SELECT ano FROM eleicao WHERE ano={ano})')

# Obter id do turno
cursor.execute(f"SELECT id FROM turno WHERE numero={data.iloc[0]['NR_TURNO']}")
turno_id = cursor.fetchone()[0]

# Obtém id da eleição
cursor.execute(f"SELECT id FROM eleicao WHERE ano={ano}")
eleicao_id = cursor.fetchone()[0]


# ----- Inicializa variáveis para otimização
local_id, municipio, cargo = "", "", ""
cpfs = []
candidato_ids = []
votos = []

start_time = time.time()
data_size = len(data_candidatos_municipios)
for index, row in data_candidatos_municipios.iterrows():
    # Aceita apenas votos Nominal, Nulo e Branco
    tipo_votavel = row['DS_TIPO_VOTAVEL']
    if tipo_votavel not in ('Nominal', 'Nulo', 'Branco'):
        continue

    # --- Insere local somente se for diferente do anterior para otimização
    local_change = municipio != row['NM_MUNICIPIO'].replace("'", " ")
    if local_change:
        municipio = row['NM_MUNICIPIO'].replace("'", " ")

        cursor.execute(f"INSERT INTO local (pais, uf, municipio) \
                        SELECT * FROM (SELECT 'Brasil' AS pais, '{uf}' AS uf, \
                                              '{municipio}' AS municipio) AS tmp \
                        WHERE NOT EXISTS (SELECT pais, uf, municipio FROM local \
                                          WHERE pais='Brasil' AND uf='{uf}' AND \
                                                municipio='{municipio}')")
        # Obter id do local
        cursor.execute(f"SELECT id FROM local \
                         WHERE pais='Brasil' AND uf='{uf}' AND municipio='{municipio}'")
        local_id = cursor.fetchone()[0]

        total_votos = data_municipios[municipio]

    # --- Se houve uma mudança no cargo com relação ao item anterior, calcula inválidos
    if local_change or row['DS_CARGO_PERGUNTA'] != cargo:
        cargo = row['DS_CARGO_PERGUNTA']
        votos_invalidos = data_candidatos_municipios.loc[
                            (data_candidatos_municipios['NM_MUNICIPIO'] == municipio) &
                            (data_candidatos_municipios['DS_CARGO_PERGUNTA'] == cargo) &
                            (data_candidatos_municipios['DS_TIPO_VOTAVEL'] == 'Branco')
                        ]['QT_VOTOS'].iat[0]
        votos_invalidos += data_candidatos_municipios.loc[
                                (data_candidatos_municipios['NM_MUNICIPIO'] == municipio) &
                                (data_candidatos_municipios['DS_CARGO_PERGUNTA'] == cargo) &
                                (data_candidatos_municipios['DS_TIPO_VOTAVEL'] == 'Nulo')
                            ]['QT_VOTOS'].iat[0]

    # --- Insere candidato e candidatura
    nome = row['NM_VOTAVEL'].replace("'", " ").replace("ª", ".").replace("º", ".")
    partido, numero, = row['SG_PARTIDO'], row['NR_VOTAVEL']

    if tipo_votavel in ('Nulo', 'Branco'):
        cpf = cargo + tipo_votavel[0] # Somente um candidato Branco e um Nulo por cargo
        partido = nome # Partido de Branco é Branco, de Nulo é Nulo
    else:
        # Obtém informações do candidato
        candidato = candidatos.loc[(candidatos['NM_URNA_CANDIDATO'] == nome) & 
                                   (candidatos['DS_CARGO'] == cargo) & 
                                   (candidatos['NR_CANDIDATO'] == numero) & 
                                   (candidatos['SG_PARTIDO'] == partido)]
        
        if not len(candidato):  # Gera erro se candidato não foi encontrado
            raise Exception(f"Candidato {nome} - {numero} não foi encontrado!")

        cpf = candidato.iloc[0]['NR_CPF_CANDIDATO']
    
    # Insere candidato e candidatura somente se já não estiver inserido
    if cpf not in cpfs:
        # --- Insere candidato
        cursor.execute(f"SELECT id, nome FROM candidato WHERE cpf='{cpf}'")
        curr_candidato = cursor.fetchone()

        if curr_candidato:    # Candidato já cadastrado
            candidato_id = curr_candidato[0]
            if curr_candidato[1] != nome: # Candidato mudou de nome, tratar SCD
                cursor.execute(f"UPDATE candidato SET nome='{nome}' WHERE cpf='{cpf}'")
                cursor.execute(f'INSERT INTO candidato_scd (nome, candidato_id) \
                                 VALUES ("{nome}", {candidato_id})')
        else:           # Candidato ainda não foi cadastrado
            cursor.execute(f"INSERT INTO candidato (cpf, nome) \
                             SELECT * FROM (SELECT '{cpf}' AS cpf, '{nome}' AS nome) AS tmp \
                             WHERE NOT EXISTS (SELECT cpf, nome FROM candidato \
                                               WHERE cpf='{cpf}' AND nome='{nome}')")
            # Obtém id do candidato
            cursor.execute(f"SELECT id FROM candidato WHERE cpf='{cpf}' AND nome='{nome}'")
            candidato_id = cursor.fetchone()[0]

            cursor.execute(f'INSERT INTO candidato_scd (nome, candidato_id) \
                             VALUES ("{nome}", {candidato_id})')

        # --- Insere candidatura
        cursor.execute(f"INSERT INTO candidatura \
                         SELECT * FROM \
                            (SELECT {candidato_id} AS candidato_id, {eleicao_id} AS eleicao_id, \
                                    '{cargo}' AS cargo, '{partido}' AS partido, \
                                    '{numero}' AS numero) AS tmp \
                         WHERE NOT EXISTS (SELECT * FROM candidatura \
                                           WHERE candidato_id={candidato_id} AND \
                                                 eleicao_id={eleicao_id})")
    
        cpfs.append(cpf)
        candidato_ids.append(candidato_id)
    else:
        candidato_id = candidato_ids[cpfs.index(cpf)]

    porcentagem_cargo = float(row['QT_VOTOS'] / total_votos * 100)
    porcentagem_valido_cargo = float(row['QT_VOTOS'] / (total_votos - votos_invalidos) * 100)
    
    votos.append((turno_id, eleicao_id, candidato_id, local_id, row['QT_VOTOS'], 
                  porcentagem_cargo, porcentagem_valido_cargo, turno_id, eleicao_id,
                  candidato_id, local_id))

    if (index and index % BUFFER_SIZE == 0) or (index == data_size-1):
        sql = f"INSERT INTO voto \
                SELECT * FROM \
                (SELECT %s AS turno_id, %s AS eleicao_id, \
                        %s AS candidato_id, %s AS local_id, \
                        %s AS quantidade, %s AS porcentagem_cargo, \
                        %s AS porcentagem_valido_cargo) AS tmp \
                 WHERE NOT EXISTS (SELECT * FROM voto \
                                   WHERE turno_id=%s AND eleicao_id=%s AND \
                                         candidato_id=%s AND local_id=%s);"
        
        # Insere fato somente com quantidade
        cursor.executemany(sql, votos)
        votos.clear()            

    if (index and index % SAVE_POINT == 0) or (index == data_size-1):
        conn.commit()

    if index % 100 == 0:
        time_elapsed = time.time() - start_time
        time_last = ((data_size/(index+1)) * time_elapsed) - time_elapsed
        time_last = timedelta(seconds=time_last)

    stdout.write(f"\r{((index+1)/data_size)*100:.1f}% | {index+1}/{data_size} items | {time_last} restantes")
    stdout.flush()

    