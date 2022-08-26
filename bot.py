import os
import re
import asyncio
import psycopg_pool
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


conn_info = f"""user={os.environ.get("DB_USER")}
                dbname={os.environ.get("DB_NAME")}
                password={os.environ.get("DB_PASSWORD")}
                host={os.environ.get("DB_HOST")}
                port={os.environ.get("DB_PORT")}"""
ps_pool = psycopg_pool.ConnectionPool(min_size=1, max_size=3, conninfo=conn_info)
ps_pool.open()
logger.info(ps_pool.get_stats())

app = AsyncApp(token=os.environ.get("SLACK_BOT_TOKEN"),
               # enable @app.error handler to catch the patterns
               raise_error_for_unhandled_request=True,)
client = AsyncWebClient(token=os.environ.get("SLACK_BOT_TOKEN"))
# Contains socket mode handlers that get closed on keyboard interrupt
# Manually must add to this list whenever you make a socket mode handler
socket_mode_handlers = []


async def main():
    logger.info(f"Startup at {datetime.now()}")
    try:
        # event loop is started in in if __name__ == "__main__"
        loop = asyncio.get_running_loop()
        loop.create_task(check_reminders())
    except RuntimeError as e:
        logger.critical(e)
    logger.debug("created check reminder task")
    handler = AsyncSocketModeHandler(app, os.environ["SLACK_APP_TOKEN"], )
    socket_mode_handlers.append(handler)
    logger.debug(f"Successfully created async socket mode handler: {handler}")
    await handler.start_async()
    logger.debug('after connect sleep')
    logger.debug(socket_mode_handlers)


async def is_valid_non_bot_id(user_id: str) -> bool:
    try:
        response = await client.users_info(user=user_id)
    except SlackApiError as e:
        logger.warning(f"{e}\n invalid_id: {user_id}")
        return False
    username = response['user']['name']
    is_bot = response['user']['is_bot']
    logger.debug(f"{username} is a bot? {is_bot}")
    return True if not is_bot else False


async def only_simple_channel_message(message) -> bool:
    # this coincidentally also ignores bot messages
    if 'subtype' in message:
        logger.debug(f"Not a simple channel message. Subtype is {message['subtype']}")
        return False
    else:
        logger.debug("Simple channel message: subtype is None")
        return True


async def async_filter(async_pred, iterable):
    for item in iterable:
        should_yield = await async_pred(item)
        if should_yield:
            yield item


async def check_reminders():
    await asyncio.sleep(60*10)
    asyncio.get_running_loop().create_task(check_reminders())
    logger.debug("Starting async reminder function")
    with ps_pool.connection() as conn:
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
                result = client.chat_postMessage(
                    channel=channel_id,
                    text=f"{message}\n This is a reminder that a message has not been reacted to in the past 2 days."
                )
                logger.info(result)
            except SlackApiError as err:
                logger.error(f"Error posting message: {err}")
            finally:
                conn.commit()
    logger.debug("Exiting async reminder function")


@app.error
async def handle_errors(error):
    if isinstance(error, BoltUnhandledRequestError):
        logger.info(f"Intentionally unhandled request: {error}")
        return BoltResponse(status=200, body="")
    else:
        # other error patterns
        logger.debug(error)
        return BoltResponse(status=500, body="Something Wrong")


# tracks channel messages with non-bot user mentions
# matchers=[only_simple_channel_message]
@app.message(re.compile('<@([UW][A-Za-z0-9]+)>'))
async def handle_user_mention_message(context, message):
    """Will likely need a switch here to handle an announcement in #general"""
    # context['matches'] is a tuple with captured User IDs.
    # Transform tuple to set to remove potential duplicate User IDs
    raw_mentions_by_id = set(context['matches'])
    # Remove bot mentions just in case
    logger.debug(f"All mentioned users: {raw_mentions_by_id}")
    # This must be a list so psycopg can adapt it to a postgres array value
    user_mentions_by_id = [i async for i in async_filter(is_valid_non_bot_id, raw_mentions_by_id)]
    # user_mentions_by_id.append("LOLOOL")
    logger.opt(colors=True).debug(f"Mentioned <red>human</red> users: {user_mentions_by_id}")
    channel_id = message['channel']
    message_ts = message['event_ts']
    two_days_in_unix_seconds = 3600 * 24 * 2
    logger.debug(f"{channel_id} {message_ts} {user_mentions_by_id}")
    remind_time = int(float(message_ts)) + two_days_in_unix_seconds
    remind_time = datetime.utcfromtimestamp(remind_time)
    with ps_pool.connection() as conn:
        conn.execute("""INSERT INTO
                     mention_message(channel_id, message_ts, remind_time, nonresponder_ids)
                     VALUES(%s, %s, %s, %s)""",
                     (channel_id, message_ts, remind_time, user_mentions_by_id, ))
        conn.commit()


# listens to reactions, but only does db ops if associated message has mentions
@app.event("reaction_added")
async def handle_reaction_for_mention_message(payload):
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
        match = re.search(r"<@([UW][A-Za-z0-9]+)>", message['text'])
        if not match:
            logger.debug("reaction detected on non-mention msg, ignoring reaction")
            return
    except SlackApiError as err:
        logger.error(f"Error fetching message associated with reaction: {err}")
    user_id = payload['user']
    channel_id = payload['item']['channel']
    message_ts = payload['item']['ts']
    reaction = payload['reaction']
    logger.debug(f"Valid reaction: user-{user_id} rxn-{reaction} chan_id-{channel_id} ts-{message_ts}")
    with ps_pool.connection() as conn:
        conn.execute("""UPDATE mention_message
                        SET nonresponder_ids = array_remove(nonresponder_ids, %s)""", (user_id, ))
        conn.commit()


async def safe_shutdown(handlers: List[AsyncSocketModeHandler], ps_conn_pool: psycopg_pool.ConnectionPool):
    logger.debug("Initiating graceful shutdown.")
    await shutdown_handlers(handlers)
    try:
        ps_conn_pool.close()
    except psycopg_pool.errors as errs:
        logger.error(f"Error closing postgres connection pool connection. {errs}")
    logger.debug("Postgres connection pool closed.")
    logger.debug("Graceful shutdown process ended.")


async def shutdown_handlers(handlers: List[AsyncSocketModeHandler]):
    for handler in handlers:
        await handler.close_async()
        logger.debug(f"{handler} shutting down")
    logger.debug("Initialized async handlers shut down.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt detected. Attempting to gracefully shutdown.")
        asyncio.run(safe_shutdown(socket_mode_handlers, ps_pool))
