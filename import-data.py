import mysql.connector
import pandas as pd
import numpy as np

# Conexão com MySQL
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
cursor = conn.cursor()

# Leitura dados votos
data = pd.read_csv('./data/2022-SC.csv', sep=';', encoding='cp1252', nrows=10,
                   usecols=['ANO_ELEICAO', 'NR_TURNO', 'SG_UF', 'NM_MUNICIPIO',
                            'NR_ZONA', 'NR_SECAO', 'DS_CARGO_PERGUNTA', 'NR_VOTAVEL', 
                            'SG_PARTIDO', 'NM_VOTAVEL', 'QT_VOTOS', 'QT_COMPARECIMENTO',
                            'DS_TIPO_VOTAVEL'],
                   dtype={'ANO_ELEICAO': np.int16, 'NR_TURNO': np.int8, 'NR_ZONA': np.int32,
                          'NR_SECAO': np.int32, 'NR_VOTAVEL': str, 'QT_VOTOS': np.int16, 
                          'QT_COMPARECIMENTO': np.int32})
data['DS_CARGO_PERGUNTA'] = data['DS_CARGO_PERGUNTA'].str.upper()
print(f'File:\n{data}\n')

# Leitura candidatos
candidatos = pd.read_csv('./data/2022-candidatos.csv', sep=';', encoding='cp1252',
                          usecols=['DS_CARGO', 'NM_URNA_CANDIDATO', 'NR_CPF_CANDIDATO',
                                   'SG_PARTIDO', 'NR_CANDIDATO'],
                          dtype={'NR_CPF_CANDIDATO': str, 'NR_CANDIDATO': str})

print(f'\n{candidatos}\n')

# Insere eleição
ano = data['ANO_ELEICAO'][0]
cursor.execute(f'INSERT INTO eleicao (ano) \
                 SELECT * FROM (SELECT {ano} AS ano) AS tmp \
                 WHERE NOT EXISTS (SELECT ano FROM eleicao WHERE ano={ano})')

# Iterar sobre votos
for index, row in data.iterrows():
    # Aceita apenas votos Nominal, Nulo e Branco
    tipo_votavel = row['DS_TIPO_VOTAVEL']
    if tipo_votavel not in ('Nominal', 'Nulo', 'Branco'):
        continue

    # Insere local
    uf, municipio, zona, secao = row['SG_UF'], row['NM_MUNICIPIO'], row['NR_ZONA'], row['NR_SECAO']
    cursor.execute(f"INSERT INTO local (pais, uf, municipio, zona, secao) \
                     SELECT * FROM (SELECT 'Brasil' AS pais, '{uf}' AS uf, \
                                           '{municipio}' AS municipio, '{zona}' AS zona, \
                                           '{secao}' AS secao) AS tmp \
                     WHERE NOT EXISTS (SELECT pais, uf, municipio, zona, secao FROM local \
                                       WHERE pais='Brasil' AND uf='{uf}' AND \
                                             municipio='{municipio}' AND zona='{zona}' AND \
                                             secao='{secao}')")
    
    # Insere candidato e candidatura
    nome, cargo, = row['NM_VOTAVEL'].replace("'", " "), row['DS_CARGO_PERGUNTA']
    partido, numero = row['SG_PARTIDO'], row['NR_VOTAVEL']

    if tipo_votavel in ('Nulo', 'Branco'):
        cpf = cargo # Somente um candidato Branco e um Nulo por cargo
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

    # Insere candidato
    cursor.execute(f"INSERT INTO candidato (cpf, nome) \
                     SELECT * FROM (SELECT '{cpf}' AS cpf, '{nome}' AS nome) AS tmp \
                     WHERE NOT EXISTS (SELECT cpf, nome FROM candidato \
                                       WHERE cpf='{cpf}' AND nome='{nome}')")
    
    # Insere candidatura
    cursor.execute(f"SELECT id FROM candidato WHERE cpf='{cpf}' AND nome='{nome}'")
    candidato_id = cursor.fetchone()[0]

    cursor.execute(f"SELECT id FROM eleicao WHERE ano={ano}")
    eleicao_id = cursor.fetchone()[0]

    cursor.execute(f"INSERT INTO candidatura \
                     SELECT * FROM \
                        (SELECT {candidato_id} AS candidato_id, {eleicao_id} AS eleicao_id, \
                                '{cargo}' AS cargo, '{partido}' AS partido, \
                                '{numero}' AS numero) AS tmp \
                     WHERE NOT EXISTS (SELECT * FROM candidatura \
                                       WHERE candidato_id={candidato_id} AND \
                                             eleicao_id={eleicao_id})")

conn.commit()
conn.close()
