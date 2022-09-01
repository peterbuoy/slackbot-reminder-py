import configparser
import os
from dotenv import load_dotenv
load_dotenv()

config = configparser.ConfigParser()
env = os.environ.get("ENV")
if env.lower() == "dev":
    config.read('app_config.dev.ini')
elif env.lower() == "prod":
    config.read('app_config.prod.ini')
else:
    pass


async def only_simple_non_bot_channel_message(message, next):
    # this coincidentally also ignores integration bot messages
    if 'subtype' not in message:
        await next()


async def only_general_channel_message(message, next):
    if message['channel'] == config['id']['channel_id_general']:
        await next()


async def only_boss_message(message, next):
    if message['user'] == config['id']['user_id_boss']:
        await next()
