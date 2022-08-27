import os
import re
import asyncio
import psycopg_pool
import configparser
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
config.read('app_config.ini')
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


async def infinitely_call(coro_func):
    """Dangerously call a coroutine infinitely. Make sure the coroutine has a sleep!"""
    while True:
        await coro_func()


async def check_mention_reminders(use_delay: bool = True) -> None:
    """
    Warning: changing default value can cause infinite loop\n
    Args:
        use_delay (bool): set to False to immediately call (without sleep)
    """
    seconds_to_sleep: float = (60 * 10 if use_delay else 0)
    await asyncio.sleep(seconds_to_sleep)
    logger.debug("Starting async mention checking function")
    with pg_pool.connection() as conn:
        conn.execute("""DELETE FROM mention_message
                        WHERE nonresponder_ids = '{}'""")
        reminder_messages = conn.execute("""DELETE FROM mention_message
                                            WHERE NOW() > remind_time
                                            RETURNING *""").fetchall()
        logger.debug(reminder_messages)
        for reminder in reminder_messages:
            channel_id = reminder[0]
            nonresponder_ids = reminder[3]
            message = ' '.join([f"<@{id}>" for id in nonresponder_ids])
            try:
                # prevent rate limiting for postMessage
                asyncio.sleep(1)
                result = await client.chat_postMessage(
                    channel=channel_id,
                    text=f"{message}\n This is a reminder that a message has not been reacted to in the past 2 days."
                )
                logger.info(result)
            except SlackApiError as err:
                logger.error(f"Error posting message: {err}")
            finally:
                conn.commit()
    logger.debug("Exiting async reminder function")


async def check_announcement_reminders() -> None:
    seconds_to_sleep: float = 60 * 20
    await asyncio.sleep(seconds_to_sleep)
    logger.debug("Starting async announcement checking function.")
    with pg_pool.connection() as conn:
        query = """DELETE FROM announcement_message
                WHERE NOW() > remind_time
                RETURNING responder_ids"""
        responder_ids = conn.execute(query)

    def save_non_bot_users(users_array) -> dict:
        users_store = {}
        for user in users_array:
            if user["is_bot"] is True or user["id"] == 'USLACKBOT':
                continue
            user_id = user["id"]
            users_store[user_id] = user
        return users_store
    try:
        result = await client.users_list()
        non_bot_users_store = save_non_bot_users(result["members"])
    except SlackApiError as e:
        logger.error("Error creating conversation: {}".format(e))
    nonresponder_ids = [id for id in non_bot_users_store if id not in responder_ids]
    for nonresponder_id in nonresponder_ids:
        await asyncio.sleep(2)
        try:
            result = await client.chat_postMessage(
                channel=nonresponder_id,
                text="Please remember to react to announcements in general."
            )
        except SlackApiError as e:
            logger.error(f"Error posting message: {e}")


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


async def is_simple_non_bot_channel_message(message) -> bool:
    """Returns True if the message is just a simple channel message."""
    # this coincidentally also ignores integration bot messages
    if 'subtype' in message:
        logger.debug(f"Not a simple channel message. Subtype is {message['subtype']}")
        return False
    else:
        logger.debug("Simple channel message: subtype is None")
        return True


async def is_message_in_general(message):
    # pls set this with a config
    return True if message['channel'] == config['Dev']['channel_id_general'] else False


async def is_message_author_boss(message):
    return True if message['user'] == config['Dev']['user_id_boss'] else False


async def async_filter(async_pred, iterable):
    for item in iterable:
        should_yield = await async_pred(item)
        if should_yield:
            yield item


@app.error
async def handle_errors(error):
    if isinstance(error, BoltUnhandledRequestError):
        return BoltResponse(status=200, body="")
    else:
        # other error patterns
        logger.debug(error)
        return BoltResponse(status=500, body="Something Wrong")


@app.message("<!channel>", matchers=[is_message_in_general, is_message_author_boss])
async def handle_boss_general_channel_mention(message, say):
    announcement_delay_seconds = 60
    channel_id = message['channel']
    message_ts = message['event_ts']
    responder_ids = [config['Dev']['user_id_boss']]
    remind_time = int(float(message_ts)) + announcement_delay_seconds
    remind_time = datetime.utcfromtimestamp(remind_time)
    with pg_pool.connection() as conn:
        query = """INSERT INTO announcement_message(channel_id, message_ts, remind_time, responder_ids)
                VALUES(%s, %s, %s, %s)"""
        conn.execute(query, (channel_id, message_ts, remind_time, responder_ids, ))
    await say("boss used channel mention in general!")


# tracks channel messages with non-bot user mentions
@app.message(re.compile('<@([UW][A-Za-z0-9]+)>'), matchers=[is_simple_non_bot_channel_message])
async def handle_user_mention_message(context, message):
    # context['matches'] is a tuple with captured User IDs via regex
    raw_mentions_by_id = set(context['matches'])
    logger.debug(f"All mentioned users: {raw_mentions_by_id}")
    # Remove bot mentions. Return list so psycopg can adapt it to a postgres array value
    user_mentions_by_id = [i async for i in async_filter(is_valid_non_bot_id, raw_mentions_by_id)]
    logger.opt(colors=True).debug(f"Mentioned <red>human</red> users: {user_mentions_by_id}")
    channel_id = message['channel']
    message_ts = message['event_ts']
    two_days_in_unix_seconds = 3600 * 24 * 2
    logger.debug(f"{channel_id} {message_ts} {user_mentions_by_id}")
    remind_time = int(float(message_ts)) + two_days_in_unix_seconds
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
           and user_id == config['Dev']['user_id_boss']
           and channel_id == config['Dev']['channel_id_general']):
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


async def safe_shutdown(handlers: List[AsyncSocketModeHandler],
                        ps_conn_pool: psycopg_pool.ConnectionPool) -> None:
    logger.debug("Initiating graceful shutdown.")
    await shutdown_handlers(handlers)
    try:
        await ps_conn_pool.close()
    except psycopg_pool.errors as errs:
        logger.error(f"Error closing postgres connection pool connection. {errs}")
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
