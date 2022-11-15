import mysql.connector
import pandas as pd
import numpy as np
from sys import stdout
import time
from datetime import timedelta

BUFFER_SIZE = 5000  # Tamanho do buffer de registros a serem inseridos ao mesmo tempo
SAVE_POINT = 50000  # Commit a cada SAVE_POINT registros lidos

START_POINT = 0     # Linha da planilha para iniciar a leitura
END_POINT = None    # Linha da planilha para finalizar a leitura

cpfs = []
candidato_ids = []

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

# ----- Criação do cursor
cursor = conn.cursor(buffered=True)

# ----- Leitura dados votos
data = pd.read_csv('./data/2022-SC.csv', sep=';', encoding='cp1252', # Número de linhas a serem lidas para testes
                   usecols=['ANO_ELEICAO', 'NR_TURNO', 'SG_UF', 'NM_MUNICIPIO',
                            'NR_ZONA', 'NR_SECAO', 'DS_CARGO_PERGUNTA', 'NR_VOTAVEL', 
                            'SG_PARTIDO', 'NM_VOTAVEL', 'QT_VOTOS', 'QT_COMPARECIMENTO',
                            'DS_TIPO_VOTAVEL'],
                   dtype={'ANO_ELEICAO': np.int16, 'NR_TURNO': np.int8, 'NR_ZONA': np.int32,
                          'NR_SECAO': np.int32, 'NR_VOTAVEL': str, 'QT_VOTOS': np.int16, 
                          'QT_COMPARECIMENTO': np.int32})
if END_POINT:
    data = data[START_POINT:END_POINT]
else:
    data = data[START_POINT:]

data_size = len(data)                          
data['DS_CARGO_PERGUNTA'] = data['DS_CARGO_PERGUNTA'].str.upper() # Cargo CAIXA ALTA
print(f'File:\n{data}\n')

# ----- Leitura candidatos
print("Candidatos:")
candidatos = pd.read_csv('./data/2022-candidatos.csv', sep=';', encoding='cp1252',
                          usecols=['DS_CARGO', 'NM_URNA_CANDIDATO', 'NR_CPF_CANDIDATO',
                                   'SG_PARTIDO', 'NR_CANDIDATO'],
                          dtype={'NR_CPF_CANDIDATO': str, 'NR_CANDIDATO': str})

print(f'\n{candidatos}\n')

# ----- Insere eleição
ano = data['ANO_ELEICAO'][0]
cursor.execute(f'INSERT INTO eleicao (ano) \
                 SELECT * FROM (SELECT {ano} AS ano) AS tmp \
                 WHERE NOT EXISTS (SELECT ano FROM eleicao WHERE ano={ano})')

# Obter id do turno
cursor.execute(f"SELECT id FROM turno WHERE numero={data.iloc[0]['NR_TURNO']}")
turno_id = cursor.fetchone()[0]

# Obtém id da eleição
cursor.execute(f"SELECT id FROM eleicao WHERE ano={ano}")
eleicao_id = cursor.fetchone()[0]

# Inicializa variáveis para otimização
local_id, uf, municipio, zona, secao, cargo = "", "", "", "", "", ""
insert = False  # Autorização para inserir dados no banco
votos_candidatos_cargo = [] # Guarda registros de voto
votos_invalidos = 0     # Quantidade de votos inválidos

votos = []
start_time = time.time()

# ----- Iterar sobre votos
for index, row in data.iterrows():

    # Aceita apenas votos Nominal, Nulo e Branco
    tipo_votavel = row['DS_TIPO_VOTAVEL']
    if tipo_votavel not in ('Nominal', 'Nulo', 'Branco'):
        continue
    
    # --- Insere local somente se for diferente do anterior para otimização
    local_change = (uf, municipio, zona, secao) != (row['SG_UF'], row['NM_MUNICIPIO'].replace("'", " "), row['NR_ZONA'], row['NR_SECAO'])
    if local_change:
        uf, municipio, zona, secao = row['SG_UF'], row['NM_MUNICIPIO'].replace("'", " "), row['NR_ZONA'], row['NR_SECAO']
        cursor.execute(f"INSERT INTO local (pais, uf, municipio, zona, secao) \
                        SELECT * FROM (SELECT 'Brasil' AS pais, '{uf}' AS uf, \
                                            '{municipio}' AS municipio, '{zona}' AS zona, \
                                            '{secao}' AS secao) AS tmp \
                        WHERE NOT EXISTS (SELECT pais, uf, municipio, zona, secao FROM local \
                                        WHERE pais='Brasil' AND uf='{uf}' AND \
                                                municipio='{municipio}' AND zona='{zona}' AND \
                                                secao='{secao}')")
        # Obter id do local
        cursor.execute(f"SELECT id FROM local \
                         WHERE pais='Brasil' AND uf='{uf}' AND municipio='{municipio}' AND \
                               zona='{zona}' AND secao='{secao}'")
        local_id = cursor.fetchone()[0]
    
        total_votos = row['QT_COMPARECIMENTO']
    
    # --- Se houve uma mudança no cargo com relação ao item anterior, calcula métricas
    if local_change or row["DS_CARGO_PERGUNTA"] != cargo:
        votos_validos = total_votos - votos_invalidos
        for candidato in votos_candidatos_cargo:
            candidato[5] = candidato[4] / total_votos * 100
            candidato[6] = candidato[4] / votos_validos * 100
            votos.append(tuple(candidato))

        if index and insert:    # Insere registros se autorizado
            # --- Inserir tabela fato
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
            insert = False

        votos_candidatos_cargo = []
        votos_invalidos = 0

    # --- Insere candidato e candidatura
    nome = row['NM_VOTAVEL'].replace("'", " ").replace("ª", ".").replace("º", ".")
    partido, numero, cargo = row['SG_PARTIDO'], row['NR_VOTAVEL'], row['DS_CARGO_PERGUNTA']

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

    if tipo_votavel == 'Branco' or tipo_votavel == 'Nulo':  # Calcula votos inválidos para o cargo
        votos_invalidos += row['QT_VOTOS']

    # Insere votos do cargo
    votos_candidatos_cargo.append([turno_id, eleicao_id, candidato_id, local_id, row['QT_VOTOS'],
                                   None, None, turno_id, eleicao_id, candidato_id, local_id])
    
    if index and index % BUFFER_SIZE == 0:
        insert = True

    if index and index % SAVE_POINT == 0:
        conn.commit()

    if index % 100 == 0:
        time_elapsed = time.time() - start_time
        time_last = ((data_size/(index+1)) * time_elapsed) - time_elapsed
        time_last = timedelta(seconds=time_last)

    stdout.write(f"\r{((index+1)/data_size)*100:.1f}% | {index+1}/{data_size} items | {time_last} restantes")
    stdout.flush()

# --- Inserir tabela fato
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

print(f"\nTempo de execução: {timedelta(seconds=(time.time() - start_time))}")

conn.commit()
conn.close()
