import requests
import json
import configparser
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta


config = configparser.ConfigParser()
config.read('config.ini')

URL = config.get('APPLICATION', 'URL')
API_VERSION = config.get('APPLICATION', 'API_VERSION')
TOKEN = config.get('APPLICATION', 'TOKEN')
CHANNEL_ID = config.get('APPLICATION', 'CHANNEL_ID')

CONNECTION = psycopg2.connect(dbname=config.get('DATABASE', 'DB_NAME'), user=config.get('DATABASE', 'DB_USER'),
                            password=config.get('DATABASE', 'DB_PASSWORD'), host=config.get('DATABASE', 'DB_HOST'))
CURSOR = CONNECTION.cursor()


def get_messages_of_dialog_id(dialog_id, response_limit):
    method = "messages?"
    input_params = "dialog_id=" + dialog_id + "&limit=" + str(response_limit)
    full_url = URL + API_VERSION + method + input_params
    response = requests.get(full_url, headers={"Authorization": TOKEN})
    json_obj = json.loads(response.content)

    for message in json_obj['data']:
        print(message['id'])


def get_messages_by_dates(channel_id, start_date, finish_date):
    method = "messages?"
    input_params = "channel_id=" + channel_id + "&start_date=" + start_date + "&finish_date=" + finish_date
    full_url = URL + API_VERSION + method + input_params
    response = requests.get(full_url, headers={"Authorization": TOKEN})
    json_obj = json.loads(response.content)

    for message in json_obj['data']:
        save_massages_to_db(message)


def save_massages_to_db(message):
    CONNECTION.autocommit = True
    if check_duplicate_id_in_messages(message['id']) is not True:

        values = [
            (message['id'], message['text'], message['transport'], message['type'], message['read'],
             message['created'],
             message['channel_id'],
             message['dialog_id'], message['client_id'], message['request_id'],
             message['operator_id'], message['is_new'])
        ]
        print(values)
        insert = sql.SQL('INSERT INTO c2d_messages '
                         '(id, "text", transport, "type", "read", created, '
                         'channel_id, dialog_id, client_id, request_id, '
                         'operator_id, is_new) VALUES {}').format(sql.SQL(',').join(map(sql.Literal, values))
        )
        CURSOR.execute(insert)
    else:
        print('message id: ' + str(message['id']) + ' is exists. Skipping!')


def check_duplicate_id_in_messages(message_id):
    CURSOR.execute('SELECT count(*) FROM c2d_messages where id=' + str(message_id))
    result = False
    for row in CURSOR:
        if row[0] == 0:
            result = False
        else:
            result = True
    return result


start_date = "01-12-2020"
for i in range(1, 32):
    date = datetime.strptime(start_date, "%d-%m-%Y")
    days_delta = i
    modified_date = date + timedelta(days=days_delta)
    search_date = datetime.strftime(modified_date, "%d-%m-%Y")
    get_messages_by_dates(CHANNEL_ID, search_date, search_date)

CURSOR.close()
CONNECTION.close()
