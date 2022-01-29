# Chat reader module v1.10
# 29/01/2022
# https://t.me/ssleg  © 2021-2022

import logging
from asyncio import sleep
from datetime import datetime

from psycopg2 import extensions
from telethon import TelegramClient, types, errors

user_dict = {}
channel_dict = {}

client: TelegramClient
con: extensions.connection
cursor: extensions.cursor


# обновление информации о пользователях
async def update_user(user_id):
    user_info = await client.get_entity(user_id)
    user_dict[user_id] = 0

    entry = (user_id, user_info.first_name, user_info.last_name, user_info.username, user_info.phone, user_info.bot,
             user_info.deleted)
    cursor.execute('''insert into chat_reader_users (user_id, first_name, last_name, user_name, phone, is_bot, is_dead)
        values (%s, %s, %s, %s, %s, %s, %s)''', entry)

    con.commit()


# обновление информации о каналах/чатах
async def update_channel(channel_id):
    channel_dict[channel_id] = 0
    try:
        channel_info = await client.get_entity(channel_id)

        entry = (channel_id, channel_info.title, channel_info.username)
        cursor.execute('''insert into chat_reader_channels (channel_id, title, user_name)
                values (%s, %s, %s)''', entry)

    except errors.ChannelPrivateError:
        entry = (channel_id, 'PRIVATE_CHANNEL')
        cursor.execute('insert into chat_reader_channels (channel_id, title) VALUES (%s,%s)', entry)

    except Exception as e:
        levent = f'channel error {channel_id}, {e}'
        print(levent)
        logging.error(levent)

    con.commit()


# загрузка в базу limit сообщений
async def read_messages(chat_id, start_id, limit):
    read_count = 0
    last_read_id = 0
    async for message in client.iter_messages(chat_id, reverse=True, limit=limit, min_id=start_id):
        read_count += 1
        message_id = message.id
        last_read_id = message_id
        message_date = message.date
        message_text = message.text
        if type(message.peer_id) == types.PeerChat:
            message_chat_id = message.peer_id.chat_id
        else:
            message_chat_id = message.peer_id.channel_id

        message_from = message.from_id
        user_id = None
        channel_id = None
        if type(message_from) == types.PeerChannel:
            channel_id = message_from.channel_id
            if channel_id not in channel_dict:
                await update_channel(channel_id)
        elif type(message_from) == types.PeerUser:
            user_id = message_from.user_id
            if user_id not in user_dict:
                await update_user(user_id)

        media_type = None
        if message.media is not None:
            media_type = str(message.media)

        reply_to = None
        reply_top = None
        if message.reply_to is not None:
            reply_to = message.reply_to.reply_to_msg_id
            reply_top = message.reply_to.reply_to_top_id
            if message.reply_to.reply_to_peer_id is not None:
                levent = f'message_peer: {message.reply_to}, {message_id}'
                print(levent)
                logging.warning(levent)

        fwd_from_user_id = None
        fwd_from_channel_id = None
        fwd_from_post = None
        fwd_from_name = None
        if message.fwd_from is not None:
            fwd_from = message.fwd_from.from_id
            if type(fwd_from) == types.PeerChannel:
                fwd_from_channel_id = message.fwd_from.from_id.channel_id
                fwd_from_post = message.fwd_from.channel_post
                if fwd_from_channel_id not in channel_dict:
                    await update_channel(fwd_from_channel_id)
            else:
                if message.fwd_from.from_id is None:
                    fwd_from_name = message.fwd_from.from_name
                else:
                    fwd_from_user_id = message.fwd_from.from_id.user_id
                    if fwd_from_user_id not in user_dict:
                        await update_user(fwd_from_user_id)

        action = None
        if message.action is not None:
            action = str(message.action)

        mess_grouped_id = message.grouped_id

        entry = (message_chat_id, message_id, user_id, channel_id, message_date, mess_grouped_id, reply_to, reply_top,
                 fwd_from_channel_id, fwd_from_post, fwd_from_user_id, fwd_from_name, message_text, media_type, action)

        cursor.execute('''insert into chat_reader_mess (chat_id, message_id, user_id, channel_id, message_date,
                        grouped_id, reply_to, reply_top, fwd_from_channel_id, fwd_from_channel_post, fwd_from_user_id,
                        fwd_from_name, message_txt, message_media, message_action)
                            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', entry)

    con.commit()
    return read_count, last_read_id


# основной цикл чтения
async def read_chat(chat_id):
    cursor.execute('select message_id from chat_reader_mess where chat_id= %s order by message_id desc limit 1',
                   (chat_id,))
    row = cursor.fetchone()
    if row is not None:
        start_read = row[0]
    else:
        start_read = 1
    summary_read = 0
    read_count = 1
    start_time = datetime.now()
    low_read_count = 0
    while read_count > 0:
        read_count, start_read = await read_messages(chat_id, start_read, 1000)
        summary_read += read_count
        levent = f'прочитано сообщений в запросе- {read_count}, последний id - {start_read}. Суммарно - {summary_read}.'
        print(levent)
        logging.info(levent)
        if read_count < 1000:
            low_read_count += 1
        if low_read_count < 5 and read_count > 0:
            await sleep(7)
        else:
            read_count = 0
    end_time = datetime.now()
    run_time = end_time - start_time
    run_seconds = round(run_time.total_seconds(), 2)
    speed = round(summary_read / run_time.total_seconds(), 1)
    # noinspection SpellCheckingInspection
    levent = f'время закачки - {run_seconds} секунд. Cкорость {speed} сообщений/с.'
    print(levent)
    logging.info(levent)
    return summary_read


# загрузка словарей пользователей и каналов
async def init(tg_client, connection, con_cursor):
    global client
    global con
    global cursor
    client = tg_client
    con = connection
    cursor = con_cursor

    cursor.execute('select user_id from chat_reader_users')
    for user in cursor.fetchall():
        user_dict[user[0]] = 0

    cursor.execute('select channel_id from chat_reader_channels')
    for channel in cursor.fetchall():
        channel_dict[channel[0]] = 0

    levent = f'словарь юзеров - {len(user_dict)}, каналов - {len(channel_dict)}.'
    print(levent)
    logging.info(levent)
