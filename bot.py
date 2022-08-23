import os
import asyncio
import re
import sys
import psycopg_pool
from loguru import logger
from datetime import datetime
from slack_bolt import BoltResponse
from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.aiohttp import SocketModeHandler
from slack_bolt.error import BoltUnhandledRequestError
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
load_dotenv()

ps_pool = psycopg_pool.ConnectionPool(min_size=1, max_size=3,
                                      conninfo=f"""user={os.environ.get("DB_USER")}
                                      dbname={os.environ.get("DB_NAME")}
                                      password={os.environ.get("DB_PASSWORD")}
                                      host={os.environ.get("DB_HOST")}
                                      port={os.environ.get("DB_PORT")}
                                      dbname={os.environ.get("DB_NAME")}""")
ps_pool.open()
logger.info(ps_pool.get_stats())

app = AsyncApp(token=os.environ.get("SLACK_BOT_TOKEN"),
               # enable @app.error handler to catch the patterns
               raise_error_for_unhandled_request=True,)
client = AsyncWebClient(token=os.environ.get("SLACK_BOT_TOKEN"))


def is_valid_non_bot_id(user_id: str, client: AsyncWebClient) -> bool:
    try:
        response = client.users_info(user=user_id)
    except SlackApiError as e:
        logger.warning(f"{e}\n invalid_id: {user_id}")
        return False
    username = response['user']['name']
    is_bot = response['user']['is_bot']
    logger.debug(f"{username} is a bot? {is_bot}")
    return True if not is_bot else False


def only_simple_channel_message(message) -> bool:
    # this coincidentally also ignores bot messages
    if 'subtype' in message:
        logger.debug(f"Not a simple channel message. Subtype is {message['subtype']}")
        return False
    else:
        logger.debug("Simple channel message: subtype is None")
        return True


async def check_reminders():
    await asyncio.sleep(10)
    asyncio.get_running_loop().create_task(check_reminders())
    logger.debug(f"Starting async function: {sys._getframe().f_code.co_name}")
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
            # consider switching to asyncio and adding sleeps to prevent rate limiting
            try:
                result = client.chat_postMessage(
                    channel=channel_id,
                    text=f"{message}\n This is a reminder that a message has not been reacted to in the past 2 days."
                )
                logger.info(result)
            except SlackApiError as err:
                logger.error(f"Error posting message: {err}")
            finally:
                conn.commit()
    logger.debug(f"Exiting async function: {sys._getframe().f_code.co_name}")


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
@app.message(re.compile('<@([UW][A-Za-z0-9]+)>'), matchers=[only_simple_channel_message])
async def handle_user_mention_message(context, message):
    """Will likely need a switch here to handle an announcement in #general"""
    # context['matches'] is a tuple with captured User IDs.
    # Transform tuple to set to remove potential duplicate User IDs
    raw_mentions_by_id = set(context['matches'])
    # Remove bot mentions just in case
    logger.debug(f"All mentioned users: {raw_mentions_by_id}")
    # This must be a list so psycopg can adapt it to a postgres array value
    user_mentions_by_id = list(filter(is_valid_non_bot_id, raw_mentions_by_id))
    # user_mentions_by_id.append("LOLOOL")
    logger.opt(colors=True).debug(f"Mentioned <red>human</red> users: {user_mentions_by_id}")
    channel_id = message['channel']
    message_ts = message['event_ts']
    two_days_in_unix_seconds = 3600 * 24 * 2
    logger.debug(f"{message_ts}, {int(float(message_ts))}")
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
        result = client.conversations_history(
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


async def main():
    logger.info(f"Startup at {datetime.now()}")
    try:
        asyncio.get_running_loop().create_task(check_reminders())
    except RuntimeError as e:
        logger.critical(e)
    await SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"], ).start_async()


if __name__ == "__main__":
    asyncio.run(main())
