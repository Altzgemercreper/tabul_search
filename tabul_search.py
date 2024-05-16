from pyspark.sql import SparkSession
from pyspark import SparkConf

db_name = '...'

tables = spark.catalog.listTables(dbName = db_name)     #Ввести нужную бд или схему

v_tables = []

#print('Характеристики таблиц') #<---------------------------------(1)
for table in tables:
    if 'v_' in table:
        v_tables.append(table)  #Сортировка
    #print(table)   #<---------------------------------(1)
    
name_tables = []

for t in v_tables:
    name_tables.append(t[0])    #Вывод списка таблиц

print('Список v_tables')     
print(name_tables)

tabul_select = []
tabul = []
no_tabul = []

#print('Вывод символов из sql:')        #<--------------------------------(2)
for t in name_tables:
    df = spark.sql('show create table' + f'{db_name}' + f'{t}')     #Сортировка на наличие табуляций
    tab = df.collect()
    #print(tab)     #<--------------------------------(2)
    if any ('\nselect \t' in str(row) for row in tab):
        #print('НАШЁЛ')     #<--------------------------------(2)
        tabul_select.append(t)
    elif any ('\t' in str(row) for row in tab):
        #print('ЛЮБАЯ ТАБУЛЯЦИЯ')       #<--------------------------------(2)
        tabul.append(t)
    else:
        #print('НЕ НАШЁЛ')      #<--------------------------------(2)
        no_tabul.append(t)
        
print('Таблицы с табуляцией в select:\n', tabul_select)     #Вывод списков из
print('Таблицы с табуляцией не в select:\n', tabul)
print('Таблицы без табуляций:\n', no_tabul)
print()
print('Количество таблиц с табуляцией в select:', len(tabul_select))
print('Количество таблиц с табуляцией не в select:', len(tabul))
print('Количество таблиц с табуляцией:', len(tabul) + len(tabul_select))
print('Количество таблиц без табуляций:', len(no_tabul))
print('Количество представлений:', len(name_tables))
print('Количество всех представлений в сумме:', len(tabul) + len(tabul_select) + len(no_tabul))     #Контрольная сумма