#!/usr/bin/env python3

# Telegram chat reader v1.00
# 21/12/2021
# https://t.me/ssleg  © 2021


import logging
from os import path
from sys import argv

import psycopg2
import toml
from telethon import TelegramClient, functions, errors

import reader_module

log_name = ''

database = ''
database_host = ''
database_user = ''
database_pass = ''
database_port = ''

api_id = 0
api_hash = ''

check_mess = '''
Сначала введите свои настройки telegram и базы данных в chat_reader.toml \
и запустите скрипт с ключом --check.

Полный список команд, смотрите --help. 
'''

# noinspection SpellCheckingInspection
help_mess = '''
Команды:
--check - проверяет настройки бд и telegram.
--all - скачивает обновления для всех чатов, которые ранее скачивались в бд.
--сhat name - добавляет и скачивает чат @name или чат, подключенный к каналу @name
'''


# версия программы из заголовка
def get_version():
    my_name = path.basename(__file__)
    file = open(my_name)
    version = ''
    for line in file:
        line = line[0:len(line) - 1]
        if len(line) > 0:
            if line[0] == '#':
                offset = line.find(' v')
                if offset > -1:
                    version = line[offset + 1:len(line)]
                    break
    file.close()
    return version


# форматирование чисел с пробелом
def set_num_printable(number):
    string = '{:,}'.format(number)
    string = string.replace(',', ' ')
    return string


# словарь чатов в базе данных
def get_db_chats_dict(curs):
    curs.execute('''
     select title, channel_id from chat_reader_channels as crc
     right join
         (select chat_id from chat_reader_mess group by chat_id) as temp
     on crc.channel_id=temp.chat_id
     ''')

    chats_dict = {}

    for row in curs.fetchall():
        chats_dict[row[1]] = row[0]

    return chats_dict


# обновление всех чатов
def update_all():
    client = TelegramClient('chat_reader', api_id, api_hash)

    con = psycopg2.connect(database=database,
                           user=database_user,
                           password=database_pass,
                           host=database_host,
                           port=database_port)
    cursor = con.cursor()

    chats_dict = get_db_chats_dict(cursor)

    async def update():
        await reader_module.init(client, con, cursor)

        summary_read = 0
        for key in chats_dict:
            levent = f'читаем {chats_dict[key]}...'
            print(levent)
            logging.info(levent)
            summary_read += await reader_module.read_chat(key)
            print('')

        levent = f'Всего прочитано: {set_num_printable(summary_read)}.'
        print(levent)
        logging.info(levent)

    client.start()
    client.loop.run_until_complete(update())

    client.disconnect()
    con.close()


# статистика базы данных
def print_stats():
    con = psycopg2.connect(database=database,
                           user=database_user,
                           password=database_pass,
                           host=database_host,
                           port=database_port)
    cursor = con.cursor()

    cursor.execute('select count(chat_id), chat_id from chat_reader_mess group by chat_id')

    chats_count = 0
    messages = 0
    for row in cursor.fetchall():
        messages += row[0]
        chats_count += 1

    print(f'В базе {set_num_printable(messages)} сообщений из {chats_count} чатов.')

    cursor.execute("select pg_total_relation_size('chat_reader_mess')")
    row = cursor.fetchone()
    size_of = row[0]
    cursor.execute("select pg_total_relation_size('chat_reader_users')")
    row = cursor.fetchone()
    size_of += row[0]
    cursor.execute("select pg_total_relation_size('chat_reader_channels')")
    row = cursor.fetchone()
    size_of += row[0]
    size_mb = round(size_of / 1048576, 2)
    print(f'Размер таблиц {set_num_printable(size_mb)} Мб.')

    con.close()


# проверка конфигурации и создание таблиц в базе
def check_config():
    valid_db = False
    try:
        con = psycopg2.connect(database=database,
                               user=database_user,
                               password=database_pass,
                               host=database_host,
                               port=database_port)
        valid_db = True
        cursor = con.cursor()
        cursor.execute('''
        create table if not exists chat_reader_users
        (
            user_id    bigint not null
            constraint chat_reader_users_pk
                primary key,
            first_name text,
            last_name  text,
            user_name  text,
            phone      bigint,
            is_bot     boolean,
            is_dead    boolean
        );

        create table if not exists chat_reader_channels
        (
            channel_id bigint not null
            constraint chat_reader_channels_pk
                primary key,
            title      text,
            user_name  text
        );

        create table if not exists chat_reader_mess
        (
            chat_id               bigint  not null,
            message_id            integer not null,
            user_id               bigint
            constraint chat_reader_mess_chat_reader_users_user_id_fk
                references chat_reader_users,
            channel_id            bigint
            constraint chat_reader_mess_chat_reader_channels_channel_id_fk
                references chat_reader_channels,
            message_date          timestamp(0),
            grouped_id            bigint,
            reply_to              integer,
            reply_top             integer,
            fwd_from_channel_id   bigint
            constraint chat_reader_mess_chat_reader_channels_channel_id_fk_2
                references chat_reader_channels,
            fwd_from_channel_post integer,
            fwd_from_user_id      bigint
            constraint chat_reader_mess_chat_reader_users_user_id_fk_2
                references chat_reader_users,
            fwd_from_name         text,
            message_txt           text,
            message_media         text,
            message_action        text,
            constraint chat_reader_pk
                primary key (chat_id, message_id)
        );
        ''')
        con.commit()
        con.close()

    except Exception as e:
        print(e)

    valid_client = False
    try:
        client = TelegramClient('chat_reader', api_id, api_hash)
        client.start()
        print(f'версия telethon {client.__version__}')
        valid_client = True
        client.disconnect()

    except Exception as e:
        print(e)

    toml_dict = {}
    if valid_client and valid_db:
        toml_dict['config'] = {'log_name': log_name, 'validated': True}
        print('Все настройки корректны, можно скачивать чаты.')
    else:
        toml_dict['config'] = {'log_name': log_name, 'validated': False}
    toml_dict['database'] = {'database': database, 'host': database_host, 'user': database_user,
                             'password': database_pass, 'port': database_port}
    toml_dict['telegram'] = {'api_id': api_id, 'api_hash': api_hash}
    file = open('chat_reader.toml', 'w')
    file.write(toml.dumps(toml_dict))
    file.close()


# добавление нового чата
def add_new(chat_name):
    con = psycopg2.connect(database=database,
                           user=database_user,
                           password=database_pass,
                           host=database_host,
                           port=database_port)
    cursor = con.cursor()

    client = TelegramClient('chat_reader', api_id, api_hash)

    chat_name = chat_name.lower()

    async def add_chat():
        try:
            info = await client(functions.channels.GetFullChannelRequest(chat_name))

            index = 0
            if len(info.chats) > 1:
                if info.chats[0].default_banned_rights is None:
                    index = 1
            chat_id = info.chats[index].id
            chat_title = info.chats[index].title
            chat_username = info.chats[index].username
            chats_dict = get_db_chats_dict(cursor)
            if chat_id in chats_dict:
                levent = f'Чат {chat_title} уже есть в базе.'
                print(levent)
                logging.info(levent)
            else:
                levent = f'Добавлен чат: id - {chat_id}, название - {chat_title}. читаем...'
                print(levent)
                logging.info(levent)
                cursor.execute('select channel_id from chat_reader_channels where channel_id=%s', (chat_id,))
                row = cursor.fetchone()
                if row is None:
                    entry = (chat_id, chat_title, chat_username)
                    cursor.execute('insert into chat_reader_channels (channel_id, title, user_name) values (%s,%s,%s)',
                                   entry)
                    con.commit()
                await reader_module.init(client, con, cursor)
                await reader_module.read_chat(chat_id)

        except TypeError as e:
            if str(e) == 'Cannot cast InputPeerUser to any kind of InputChannel.':
                print(f'{chat_name} это имя пользователя, а не канала или чата.')
            else:
                print(e)

        except ValueError as e:
            if str(e) == f'No user has "{chat_name}" as username':
                print(f'{chat_name} это имя не используется в telegram.')
            elif str(e) == f'Cannot find any entity corresponding to "{chat_name}"':
                print(f'{chat_name} это имя никогда не использовалось в telegram.')
            else:
                print(e)

        except errors.ChannelPrivateError:
            # noinspection SpellCheckingInspection
            print(f'Чат/канал {chat_name} помечен как приватный. Возможно, вас там забанили.')

        except Exception as e:
            print(e)

    client.start()
    client.loop.run_until_complete(add_chat())

    client.disconnect()
    con.close()


# точка входа
if __name__ == '__main__':
    param_dict = toml.load('chat_reader.toml')
    config = param_dict['config']
    db = param_dict['database']
    tg = param_dict['telegram']

    log_name = config['log_name']

    database = db['database']
    database_host = db['host']
    database_user = db['user']
    database_pass = db['password']
    database_port = db['port']

    api_id = tg['api_id']
    api_hash = tg['api_hash']

    print(f'Telegram chat reader {get_version()}.')

    validated = config.get('validated')

    if len(argv) == 1:
        if validated is None:
            print(check_mess)
            exit(1)
        elif validated is True:
            print_stats()
            exit(0)
        else:
            print('Проверьте правильность конфигурации chat_reader.toml и запустите с ключом --check')
            exit(1)

    if argv[1] == '--help':
        print(help_mess)

    elif argv[1] == '--check':
        check_config()

    elif argv[1] == '--all':
        if validated is True:
            lfile = logging.FileHandler(log_name, 'w', 'utf-8')
            # noinspection SpellCheckingInspection
            lfile.setFormatter(logging.Formatter('%(levelname)s %(module)-13s [%(asctime)s] %(message)s'))
            logging.basicConfig(level=logging.INFO, handlers=[lfile])
            update_all()
        else:
            print(check_mess)

    elif argv[1] == '--chat':
        if validated is True:
            if len(argv) == 3:
                lfile = logging.FileHandler(log_name, 'w', 'utf-8')
                # noinspection SpellCheckingInspection
                lfile.setFormatter(logging.Formatter('%(levelname)s %(module)-13s [%(asctime)s] %(message)s'))
                logging.basicConfig(level=logging.INFO, handlers=[lfile])
                add_new(argv[2])
            else:
                print('не указано имя чата/канала.')
        else:
            print(check_mess)

    else:
        print('команда не понята.')
        print(help_mess)