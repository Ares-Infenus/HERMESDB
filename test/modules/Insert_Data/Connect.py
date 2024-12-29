import cx_Oracle # Import the cx_Oracle module

try:
    connection=cx_Oracle.connect(
        user='HERMES_DB',
        password='1234567', 
        dsn='192.168.1.120:1521/XEPDB1',
        encoding='UTF-8')
    print(connection.version)
except Exception as ex:
    print(ex) 
