# myscript.py

import cx_Oracle

connection = cx_Oracle.connect(user="focco3i", password="BZ2UE4cYnPVcqqE",
                               dsn="192.168.1.5/f3ipro")

cursor = connection.cursor()
cursor.execute("""
        SELECT COD_EMP, RAZAO_SOCIAL FROM TEMPRESAS 
        WHERE id > :eid""",
        eid = 1)
for fname, lname in cursor:
    print("Values:", fname, lname)