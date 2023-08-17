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

    with open('C:/neostudy/1.3/f_101.csv') as f:
        next(f) #пропуск первой строки с заголовками
        cursor.execute('set search_path to dm')
        cursor.copy_from(f, 'dm_f101_round_f_v2', sep=',', null='')
        connection.commit()

except (Exception, Error) as error:
    print(f'Ошибка при работе с Postgres - {error}')
finally:
    if connection:
        cursor.close()
        connection.close()
        print('Соединение с Postgres закрыто')