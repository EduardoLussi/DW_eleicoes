import mysql.connector

# Connect to MySQL database
try:
    conn = mysql.connector.connect(host='localhost',
                                   database='DW',
                                   user='user',
                                   password='password',
                                   port=6033)
except mysql.connector.Error as err:
    print(err)

# Create cursor
cursor = conn.cursor()

cursor.execute(f"SELECT COUNT(*) FROM turno WHERE numero=1 OR numero=2")
qt_candidatos = cursor.fetchone()[0]
print(qt_candidatos+1)

conn.close()