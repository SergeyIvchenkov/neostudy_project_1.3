import csv
import psycopg2
from psycopg2 import Error

try:
    #Подключение к бд
    connection =  psycopg2.connect(user = 'postgres',
                                   password = 'serg328328',
                                   host = 'localhost',
                                   port = '5432',
                                   database = 'net_py1')
    cursor = connection.cursor()

    cursor.execute('select * from dm.dm_f101_round_f')

    headers = [desc[0] for desc in cursor.description] #шапка таблицы

    with open('C:/neostudy/1.3/f_101.csv', 'w', newline='') as csv_f:
        writer = csv.writer(csv_f)
        writer.writerow(headers)
        writer.writerows(cursor)

except (Exception, Error) as error:
    print(f'Ошибка при работе с Postgres - {error}')
finally:
    if connection:
        cursor.close()
        connection.close()
        print('Соединение с Postgres закрыто')