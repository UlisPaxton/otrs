from time import sleep
import re
import os
import logging
from requests.packages.urllib3 import disable_warnings
import mysql.connector
import configparser
from pyotrs import Client

WORK_DIRECTORY = os.getcwd()
TICKETS_PATTERN_1C = re.compile(r'(ED-\d{6})')

logging.basicConfig(filename=WORK_DIRECTORY + 'info.log', format='%(asctime)s %(message)s', level=logging.INFO)
disable_warnings()

config = configparser.ConfigParser()
config.read(WORK_DIRECTORY + 'config.ini')

DEBUG_level = int(config['GLOBAL'][
                      'DEBUG_LEVEL'])
# 0 - рабочее логирование, 1 - +логирование ошибок предоставленных данных, 2 - +логирование логики


def log(msg, verbose=0):
    """
    логирование с разбивкой по уровням, уровень подробности вызыванный в коде
    меньше или равен установленному при старте, то сообщение попадёт в лог

    """
    if verbose <= DEBUG_level:
        logging.info(msg)


def check_value(value, message='', error_message=''):
    """Проверка параметра с логированием"""
    if (value):
        log(message, verbose=1)
    else:
        log(error_message)


class Attr:
    """
    структура для конвертации словарь=объект с полями и отображения
    """

    def __init__(self, **entries):
        self.__dict__.update(entries)

    def __str__(self):
        return str(self.__dict__)


def merge(otrs_client_obj, first_tn, second_tn):
    log(f"[?]К тикету {first_tn} мёржим содержимое тикета {second_tn}")

    ticket1_id = otrs_client_obj.ticket_get_by_number(first_tn)
    ticket = otrs_client_obj.ticket_get_by_id(ticket1_id.tid, articles=True)

    if check_value((ticket1_id and ticket),
                   message=f'[+]Тикет {first_tn} найден',
                   error_message=f"CRITICAL [!!!] Не могу найти тикет {first_tn}"):
        return False  # это return из merge(), нефиг что-то пытаться делать, если нужные тикеты не найдены

    ticket2_id = otrs_client_obj.ticket_get_by_number(second_tn)
    ticket2 = otrs_client_obj.ticket_get_by_id(ticket2_id.tid, articles=True, attachments=True)

    if check_value((ticket2_id and ticket2),
                   message=f'[+]Тикет {second_tn} найден',
                   error_message=f"CRITICAL [!!!] Не могу найти тикет {second_tn}"):
        return False  # это return из merge(), нефиг что-то пытаться делать, если нужные тикеты не найдены

    for article in ticket2.articles:

        if len(article.attachments) > 0:
            log("Attachments:", verbose=1)
            log(str(article.attachments), verbose=1)
            otrs_client_obj.ticket_update(ticket.tid, article=article, attachments=article.attachments)
        else:
            otrs_client_obj.ticket_update(ticket.tid, article=article)

        sleep(float(config['otrs']['operations_interval']))

    check_value(
        otrs_client_obj.ticket_update(ticket2.tid, State="removed"),
        message=f"[+] Старыей тикет {second_tn} состояние удалённый.",
        error_message=f' CRITICAL [!!!] Не получилось сменить состояние тикета {second_tn} на "удалённый"'
    )


class MYSQLConnection:

    def __init__(self, server, user, password, db):
        self.connection = mysql.connector.connect(user=user, password=password,
                                                  host=server, database=db)
        self.cursor = None
        log(f"[+] Подключение к БД {db}  OK")

    def execute(self, query, dictionary=True):
        self.cursor = self.connection.cursor(dictionary=dictionary)
        self.cursor.execute(query)
        log(f"[+] Запрос выполнен: {query}. Результат в виде словаря {dictionary}", verbose=1)
        sql_result = self.cursor.fetchall()
        res_len = len(sql_result)
        log(f'[+] Запрос вернул {res_len} строк', verbose=1)
        self.cursor.close()
        return sql_result


Connection = MYSQLConnection(config['mysql']['host'],
                             config['mysql']['login'],
                             config['mysql']['password'],
                             config['mysql']['db_name'])

found_tickets = Connection.execute("select tn, title from ticket where (queue_id=8 and (ticket_state_id=1 or "
                                   "ticket_state_id=4)) ", dictionary=True)

tickets_1c = list()  # берём все новые тикетв из очереди с указанным queue_id ??? а если родительский тикет уже
# не новый?

for row in found_tickets:
    result = TICKETS_PATTERN_1C.findall(row['title'])  # ищем в темах номер тикета 1с
    log(f'Тема: {row["title"]}, Номер тикета 1с - {result}', verbose=2)

    if len(result) > 0:
        # если тема найдена, то создаём сущность - сопоставление номера 1с и тикета отрс
        # и помещаем в список tickets_1c
        log(f'Добавляем в tickets_1c: {row["tn"]} : {result[0]}', verbose=2)
        tickets_1c.append(Attr(**{'otrs': row['tn'], 'os': result[0]}))

linked_tickets = {}
for current_ticket in tickets_1c:

    if not current_ticket.os in linked_tickets:
        linked_tickets[current_ticket.os] = []

    for other_ticket in tickets_1c:
        #     1c                                       otrs
        if current_ticket.os == other_ticket.os and current_ticket.otrs != other_ticket.otrs:
            linked_tickets[current_ticket.os].append(other_ticket.otrs)

client = Client(config['otrs']['uri'],
                config['otrs']['login'],
                config['otrs']['password'])

if client.session_create():
    logging.info("[+] Авторизация в API OTRS ОК")

sleep(float(config['otrs']['operations_interval']))
# при несоблюдении интервала обращений к api апач может рвать соединение

for os, link in linked_tickets.items():
    dup = []

    for ticket in link:
        if ticket not in dup:
            dup.append(ticket)
            dup.sort()
        else:
            linked_tickets[os] = dup

    if len(dup) > 0:
        parrent_ticket = dup.pop(0)
        log(dup, 2)

        for ticket in dup:
            merge(client, parrent_ticket, ticket)

logging.info("end")
