import mysql.connector
import pandas as pd

# Connect to MySQL database
try:
    conn = mysql.connector.connect(host='localhost',
                                   database='DW',
                                   user='user',
                                   password='password',
                                   port=6033)
except mysql.connector.Error as err:
    print(err)

print('Connected to database!')

# Create cursor
cursor = conn.cursor()

# Read data
data = pd.read_csv('./data/2022-SC.csv', sep=';', encoding='cp1252', nrows=100,
                   usecols=['ANO_ELEICAO', 'NR_TURNO', 'SG_UF', 'NM_MUNICIPIO',
                            'NR_ZONA', 'NR_SECAO', 'DS_CARGO_PERGUNTA', 'NR_VOTAVEL', 
                            'SG_PARTIDO', 'NM_VOTAVEL', 'QT_VOTOS', 'QT_COMPARECIMENTO'])                   
print('File:')
print(data)
print()

# Insert eleicao
ano = data['ANO_ELEICAO'][0]
cursor.execute(f'INSERT INTO eleicao (ano) VALUES ({ano}) \
                 WHERE NOT EXISTS (SELECT ano FROM ELEICAO WHERE ano={ano})')

# Iterate over data
for index, row in data.iterrows():
    # Insert local
    uf, municipio, zona, secao = row['SG_UF'], row['NM_MUNICIPIO'], row['NR_ZONA'], row['NR_SECAO']
    cursor.execute(f"INSERT INTO local (pais, uf, municipio, zona, secao) \
                     VALUES ('Brasil', '{uf}', '{municipio}', '{zona}', '{secao}') \
                     WHERE NOT EXISTS (SELECT pais, uf, municipio, zona, secao FROM local \
                                       WHERE pais='Brasil', uf='{uf}' \
                                             municipio='{municipio}', zona='{zona}' \
                                             secao='{secao}')")
    
    # Insert candidato and candidatura
    nome = row['NM_VOTAVEL']

    # Get number of candidatos with nome=nome
    cursor.execute(f"SELECT COUNT(*) FROM candidato WHERE nome='{nome}'")
    qt_candidatos = cursor.fetchone()[0]

    # ...

conn.close()
