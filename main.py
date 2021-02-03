print("Импортирую библиотеки")
import os
from datetime import datetime, timedelta
import pandas as pd
from pandas.io import gbq
import gspread
import pytz
import requests
from google.cloud import bigquery
from oauth2client.service_account import ServiceAccountCredentials

print("Импортировал библиотеки")

print("Задаю хост для подключения")
URL = 'https://api.calltouch.ru/calls-service/RestAPI/requests'
print("base URL=", URL)

#дата начала
DATE_FROM = '01/01/2021'


# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


if __name__ == '__main__':
    # В переменной scope передаются разрешения для подключения к api google cloud
    scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']

    # Добавьте в корневой каталог файл creds.json полученный после регистрации сервисного аккаунта
    creds = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)

    #Авторизация в гугл таблицах
    client = gspread.authorize(creds)

    print("Авторизуюсь в гугл таблицах")


    sheet = client.open("Название вашей таблицы").sheet1 #номер листа для подключения
    print("Открыл таблицу ")
    # Получаю все данные с листа
    print("get values from sheets")
    list_of_hashes = sheet.get_all_records()


    #создаю функцию для парсинга тегов
    def join_names(request_tags: list) -> str:
        qwe = []
        for i in request_tags:
            for q in i['names']:
                qwe.append(q)
        return ','.join(qwe)

    #Создаю пустой лист
    data = []


    #Параметр date_to установлен на вчеращний день
    date_to = datetime.strftime(datetime.now(pytz.timezone('Europe/Moscow')) - timedelta(days=1), '%m/%d/%Y')

    #Начинаю цикл для который для каждого клиента в таблице формирует урл для подключения к API Calltouch, отправляет GET запрос
    #получает данные и записывает в лист data выбрать.
    print("loop for all clients request GET")
    for i in list_of_hashes:
        print("Send Get Request for ", i['Название клиента'])
        q = requests.get(URL, params={
            'clientApiId': i['Токен'],
            "dateFrom": DATE_FROM,
            "dateTo": date_to,
            "withRequestTags": True
        }).json()

        for r in q:
            print("append data from", i['Название клиента'], "to list data")
            #Здесь можно указать необходимые параметры для парсинга из request
            data.append(
                {
                    'ClientName': i['Название клиента'],
                    "ReqId": r['requestId'],
                    "Date": r['dateStr'],
                    "Tags": join_names(r['RequestTags'])
                }
            )
            print("append ", i['Название клиента'], "complete")



       os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.abspath("creds.json")
    #
    client = bigquery.Client()
    data = pd.DataFrame(data)
    print("Загружаю таблицу в BQ")

    data.to_gbq(
        destination_table='НазваниеВашегоДатасета.Название вашей таблицы',
        project_id='название вашего проекта',
        if_exists='replace' #может принимать три параметра replace - перезаписывает таблицу.
    )
    print("Загрузил таблицу в BQ")
    print("END")