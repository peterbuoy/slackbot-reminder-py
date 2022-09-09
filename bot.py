import os
import sys
import re
import asyncio
import psycopg_pool
import configparser
import middleware
from loguru import logger
from datetime import datetime
from typing import List
from slack_bolt import BoltResponse
from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.aiohttp import AsyncSocketModeHandler
from slack_bolt.error import BoltUnhandledRequestError
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
load_dotenv()

config = configparser.ConfigParser()
env = os.environ.get("ENV")
if env is None:
    logger.warning('Please specify if the environment variable ENV is "prod" or "dev"')
    sys.exit()
if env.lower() == "dev":
    config.read('app_config.dev.ini')
    logger.info(f"ENV environment variable set to {env.lower()}")
elif env.lower() == "prod":
    config.read('app_config.prod.ini')
    logger.info(f"ENV environment variable set to {env.lower()}")
else:
    logger.warning('Please specify if the environment variable ENV is "prod" or "dev"')
    sys.exit()

feature_flags = config.items('feature_flags')
logger.info("Listing feature flags below:")
for name, value in feature_flags:
    logger.info(f"{name}: {value}")

conn_info = f"""user={os.environ.get("DB_USER")}
                dbname={os.environ.get("DB_NAME")}
                password={os.environ.get("DB_PASSWORD")}
                host={os.environ.get("DB_HOST")}
                port={os.environ.get("DB_PORT")}"""

pg_pool = psycopg_pool.ConnectionPool(min_size=1, max_size=3, conninfo=conn_info)
pg_pool.open()
app = AsyncApp(token=os.environ.get("SLACK_BOT_TOKEN"),
               # enable @app.error handler to catch the patterns
               raise_error_for_unhandled_request=True,)
client = AsyncWebClient(token=os.environ.get("SLACK_BOT_TOKEN"))


async def main():
    logger.info(f"Startup at {datetime.utcnow()} UTC")
    try:
        global socket_mode_handlers
        socket_mode_handlers = []
        handler = AsyncSocketModeHandler(app, os.environ["SLACK_APP_TOKEN"], )
        socket_mode_handlers.append(handler)
        loop = asyncio.get_running_loop()
        loop.create_task(infinitely_call(check_mention_reminders))
        loop.create_task(infinitely_call(check_announcement_reminders))
        await handler.start_async()
    except RuntimeError as e:
        logger.critical(e)


def save_non_bot_users(users_array) -> dict:
    """Takes an array of user json objects via await client.users_list()"""
    users_store = {}
    for user in users_array:
        if user["is_bot"] is True or user["id"] == 'USLACKBOT':
            continue
        user_id = user["id"]
        users_store[user_id] = user
    return users_store


async def is_valid_non_bot_id(user_id: str) -> bool:
    """Hits the Slack API with a user id to determine if it valid and non-bot."""
    try:
        response = await client.users_info(user=user_id)
    except SlackApiError as e:
        logger.warning(f"{e}\n invalid_id: {user_id}")
        return False
    username = response['user']['name']
    is_bot = response['user']['is_bot']
    logger.debug(f"{username} is a bot? {is_bot}")
    return False if is_bot else True


async def async_filter(async_pred, iterable):
    for item in iterable:
        should_yield = await async_pred(item)
        if should_yield:
            yield item


if (config['feature_flags'].getboolean('boss_announcement') is True):
    @app.message("<!channel>",
                 middleware=[middleware.only_boss_message,
                             middleware.only_general_channel_message,
                             middleware.only_simple_non_bot_channel_message])
    async def handle_boss_announcement(message):
        required_response_time_seconds = int(config['required_response_time_seconds']['boss_announcement'])
        channel_id = message['channel']
        message_ts = message['event_ts']
        responder_ids = [config['id']['user_id_boss']]
        remind_time = int(float(message_ts)) + required_response_time_seconds
        remind_time = datetime.utcfromtimestamp(remind_time)
        with pg_pool.connection() as conn:
            query = """INSERT INTO announcement_message(channel_id, message_ts, remind_time, responder_ids)
                    VALUES(%s, %s, %s, %s)"""
            conn.execute(query, (channel_id, message_ts, remind_time, responder_ids, ))
        logger.debug("detected boss using channel mention in general")


if (config['feature_flags'].getboolean('user_mention') is True):
    @app.message(re.compile('<@([UW][A-Za-z0-9]+)>'),
                 middleware=[middleware.only_simple_non_bot_channel_message])
    async def handle_user_mention_message(context, message):
        # context['matches'] is a tuple with captured User IDs via regex
        raw_mentions_by_id = set(context['matches'])
        logger.debug(f"All mentioned users: {raw_mentions_by_id}")
        # Remove bot mentions. Return list so psycopg can adapt it to a postgres array value
        user_mentions_by_id = [i async for i in async_filter(is_valid_non_bot_id, raw_mentions_by_id)]
        logger.opt(colors=True).debug(f"Mentioned <red>human</red> users: {user_mentions_by_id}")
        channel_id = message['channel']
        message_ts = message['event_ts']
        required_response_time_seconds = int(config['required_response_time_seconds']['user_mention'])
        logger.debug(f"DB Insert: {channel_id} {message_ts} {user_mentions_by_id}")
        remind_time = int(float(message_ts)) + required_response_time_seconds
        remind_time = datetime.utcfromtimestamp(remind_time)
        with pg_pool.connection() as conn:
            query = """INSERT INTO mention_message(channel_id, message_ts, remind_time, nonresponder_ids)
                    VALUES(%s, %s, %s, %s)"""
            conn.execute(query, (channel_id, message_ts, remind_time, user_mentions_by_id, ))
            conn.commit()


@app.event("reaction_added")
async def handle_reactions(payload):
    """Handles reactions for mention messages and boss @channel mentions in #general"""
    # Retrieve message text from data (channel, ts) in reaction payload
    # CONVO--inclusive: oldest ts counted, oldest: only count ts after arg
    try:
        result = await client.conversations_history(
                                            channel=payload['item']['channel'],
                                            inclusive=True,
                                            oldest=payload['item']['ts'],
                                            limit=1
                                        )
        message = result["messages"][0]
        mention_match = re.search(r"<@([UW][A-Za-z0-9]+)>", message['text'])
        announcement_match = re.search("<!channel>", message['text'])
        user_id = payload['user']
        channel_id = payload['item']['channel']
        message_ts = payload['item']['ts']
        reaction = payload['reaction']
        # boss channel mention in #general
        if (announcement_match
           and user_id == config['id']['user_id_boss']
           and channel_id == config['id']['channel_id_general']):
            logger.debug("reaction detected on announcement msg")
            logger.debug(f"Valid reaction: user-{user_id} rxn-{reaction} chan_id-{channel_id} ts-{message_ts}")
            with pg_pool.connection() as conn:
                query = """UPDATE announcement_message
                        SET responder_ids = array_append(responder_ids, %s)
                        WHERE channel_id = %s and message_ts = %s
                        and responder_ids && ARRAY[%s] = false"""
                conn.execute(query, (user_id, channel_id, message_ts, user_id))
                conn.commit()
        # simple mention message
        elif mention_match:
            logger.debug("reaction detected on mention msg")
            logger.debug(f"Valid reaction: user-{user_id} rxn-{reaction} chan_id-{channel_id} ts-{message_ts}")
            with pg_pool.connection() as conn:
                query = """UPDATE mention_message
                        SET nonresponder_ids = array_remove(nonresponder_ids, %s)
                        WHERE channel_id = %s and message_ts = %s"""
                conn.execute(query, (user_id, channel_id, message_ts))
                conn.commit()
        else:
            logger.debug("Unhandled reaction detected, ignoring reaction.")
            return
    except SlackApiError as err:
        logger.error(f"Error fetching message associated with reaction: {err}")


async def infinitely_call(coro_func):
    """Dangerously call a coroutine infinitely. Make sure the coroutine has a sleep!"""
    while True:
        await coro_func()


async def check_mention_reminders() -> None:
    logger.debug("Starting async mention checking function")
    with pg_pool.connection() as conn:
        conn.execute("""DELETE FROM mention_message
                        WHERE nonresponder_ids = '{}'""")
        query = """DELETE FROM mention_message
                WHERE NOW() > remind_time
                RETURNING channel_id, nonresponder_ids"""
        reminder_messages = conn.execute(query).fetchall()
        logger.debug(reminder_messages)
        for reminder in reminder_messages:
            channel_id = reminder[0]
            nonresponder_ids = reminder[1]
            message = ' '.join([f"<@{id}>" for id in nonresponder_ids])
            try:
                # prevent rate limiting for postMessage
                await asyncio.sleep(1)
                result = await client.chat_postMessage(
                    channel=channel_id,
                    text=f"{message}\n This is a reminder that a message has not been reacted to in the past 2 days."
                )
                logger.info(result)
            except SlackApiError as err:
                logger.error(f"Error posting message: {err}")
            finally:
                conn.commit()
        conn.commit()
    logger.debug("Exiting async mention function")
    seconds_to_sleep: float = float(config['reminder_interval_in_seconds']['user_mention'])
    await asyncio.sleep(seconds_to_sleep)


async def check_announcement_reminders() -> None:
    await asyncio.sleep(2)
    logger.debug("Starting async announcement checking function.")
    with pg_pool.connection() as conn:
        query = """DELETE FROM announcement_message
                WHERE NOW() > remind_time
                RETURNING responder_ids, channel_id, message_ts"""
        returned_announcement_messages = conn.execute(query).fetchall()
    for announcement_message in returned_announcement_messages:
        responder_id_actual = announcement_message[0]
        channel_id = announcement_message[1]
        message_ts = announcement_message[2]
        try:
            result = await client.users_list()
            non_bot_users_store = save_non_bot_users(result["members"])
        except SlackApiError as e:
            logger.error("Error creating conversation: {}".format(e))
        nonresponder_ids = [id for id in non_bot_users_store if id not in responder_id_actual]
        for nonresponder_id in nonresponder_ids:
            await asyncio.sleep(2)
            try:
                result = await client.chat_postMessage(
                    channel=nonresponder_id,
                    text=f"""Please remember to react to announcements in general. \n
                           {await client.chat_getPermalink(channel=channel_id,message_ts=message_ts)['permalink']}"""
                )
            except SlackApiError as e:
                logger.error(f"Error posting message: {e}")
    seconds_to_sleep: float = float(config['reminder_interval_in_seconds']['boss_announcement'])
    logger.debug("Exiting async announcement checking function.")
    await asyncio.sleep(seconds_to_sleep)


@app.error
async def handle_errors(error):
    if isinstance(error, BoltUnhandledRequestError):
        return BoltResponse(status=200, body="")
    else:
        # other error patterns
        logger.debug(error)
        return BoltResponse(status=500, body="Something Wrong")


async def safe_shutdown(handlers: List[AsyncSocketModeHandler],
                        ps_conn_pool: psycopg_pool.ConnectionPool) -> None:
    logger.debug("Initiating graceful shutdown.")
    await shutdown_handlers(handlers)
    ps_conn_pool.close()
    logger.debug("Postgres connection pool closed.")
    logger.debug("Graceful shutdown process ended.")


async def shutdown_handlers(handlers: List[AsyncSocketModeHandler]) -> None:
    for handler in handlers:
        try:
            await handler.close_async()
        except Exception as err:
            logger.error(f"Error closing async: {err}")
        logger.debug(f"{handler} shutting down")
    logger.debug("Initialized async handlers shut down.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt detected. Attempting to gracefully shutdown.")
        asyncio.run(safe_shutdown(socket_mode_handlers, pg_pool))
