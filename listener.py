import os, requests, asyncio
import re
from telethon import TelegramClient, events, utils
from telethon.tl.functions.messages import GetDialogFiltersRequest

API_ID = int(os.environ.get("TG_API_ID"))
API_HASH = os.environ.get("TG_API_HASH")
WEBHOOK_URL = os.environ.get("N8N_WEBHOOK_URL")
SESSION_NAME = '/data/monitor_session'

# --- КЛЮЧЕВЫЕ СЛОВА ---
KEYWORDS = [
    'n8n', 'make.com', 'integromat', 'интегромат',
    'amocrm', 'амоцрм', 'amo crm',
    'bitrix', 'битрикс', 'б24', 'b24',
    'getcourse', 'геткурс',
    'yclients', 'y-clients', 'уклайнс', 'юклайнс',
    'roistat', 'роистат', 'tilda', 'тильда',
    'интеграция', 'интегрировать',
    'webhook', 'вебхук', 'api', 'json',
    'связать', 'подружить', 'автоматизация',
    'интегратор', 'технический специалист', 'техспец',
    'внедренец', 'настройщик',
    'нужен специалист', 'ищу специалиста',
    'настроить crm', 'внедрить crm',
    'подбор crm', 'выбор crm'
]

WATCH_CHATS = []

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

def get_safe_title(title_obj):
    if hasattr(title_obj, 'text'):
        return title_obj.text
    return str(title_obj)

async def update_watch_list():
    global WATCH_CHATS
    print("🔄 Сканирую папки Telegram...", flush=True)
    try:
        response = await client(GetDialogFiltersRequest())
        filters = getattr(response, 'filters', response)
        target_filter = None
        
        if filters:
            for f in filters:
                if hasattr(f, 'title'):
                    raw_title = get_safe_title(f.title)
                    if 'бизнес' in raw_title.strip().lower():
                        target_filter = f
                        break
        
        if target_filter:
            ids = []
            for peer in target_filter.include_peers:
                p_id = utils.get_peer_id(peer)
                ids.append(p_id)
            WATCH_CHATS = ids
            print(f"✅ УСПЕХ! Фильтруем {len(WATCH_CHATS)} чатов", flush=True)
        else:
            print("⚠️ Папка 'Бизнес' не найдена. Слушаю всё.", flush=True)

    except Exception as e:
        print(f"❌ Ошибка сканирования: {e}", flush=True)

@client.on(events.NewMessage(incoming=True))
async def handler(event):
    try:
        if not event.message.message: return
        
        if WATCH_CHATS and event.chat_id not in WATCH_CHATS:
            return

        text = event.message.message.lower()
        found_keys = [key for key in KEYWORDS if key in text]
        if not found_keys:
            return
            
        print(f"🎯 ЛИД: {event.message.message[:30]}...", flush=True)
        
        chat = await event.get_chat()
        sender = await event.get_sender()
        
        # --- ФОРМИРУЕМ ССЫЛКИ ---
        
        # 1. Ссылка на сообщение
        chat_username = getattr(chat, 'username', '')
        if chat_username:
            msg_link = f"https://t.me/{chat_username}/{event.message.id}"
        else:
            clean_id = str(event.chat_id).replace('-100', '')
            msg_link = f"https://t.me/c/{clean_id}/{event.message.id}"

        # 2. Ссылка на АВТОРА (Личка)
        sender_username = getattr(sender, 'username', None)
        if sender_username:
            user_link = f"https://t.me/{sender_username}"
            user_handle = f"@{sender_username}"
        else:
            # Если нет юзернейма, делаем ссылку через ID (работает внутри ТГ)
            user_link = f"tg://user?id={sender.id}"
            user_handle = "No Username"

        payload = {
            "text": event.message.message,
            "chat_title": getattr(chat, 'title', 'Unknown'),
            "sender_name": getattr(sender, 'first_name', 'Unknown'),
            "sender_link": user_link,       # <-- НОВОЕ ПОЛЕ (Ссылка на личку)
            "sender_handle": user_handle,   # <-- НОВОЕ ПОЛЕ (@username)
            "link": msg_link,
            "keywords": found_keys
        }
        
        requests.post(WEBHOOK_URL, json=payload, timeout=5)
        
    except Exception as e:
        print(f"❌ Error: {e}", flush=True)

with client:
    client.loop.run_until_complete(update_watch_list())
    print("🚀 Бот перезапущен!", flush=True)
    client.run_until_disconnected()
