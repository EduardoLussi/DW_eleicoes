# Banco de Dados MySQL

## Executar banco MySQL

Iniciar MySQL:
```
docker-compose up
```

Finalizar MySQL:
```
docker-compose down
```

## Criar banco de dados

Executar script SQL create-dabase.sql no banco de dados.

# Importar dados

1. Colocar planilhas de boletins de urna e candidatos em ./data/ e editar path no código.
2. ```python import-data.py```

# Bibliotecas

```
pip install mysql-connector-python
```

```
pip install pandas
```

```
pip install numpy
```