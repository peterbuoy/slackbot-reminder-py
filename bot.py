import os
import re
import psycopg_pool
from loguru import logger
from datetime import datetime
from slack_bolt import App, BoltResponse
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_bolt.error import BoltUnhandledRequestError
from slack_sdk import WebClient
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

app = App(token=os.environ.get("SLACK_BOT_TOKEN"),
          # enable @app.error handler to catch the patterns
          raise_error_for_unhandled_request=True,)
client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))


def isValidNonBotId(user_id: str) -> bool:
    try:
        response = client.users_info(user=user_id)
    except SlackApiError as e:
        logger.warning(f"{e}\n invalid_id: {user_id}")
        return False
    username = response['user']['name']
    is_bot = response['user']['is_bot']
    logger.debug(f"{username} is a bot? {is_bot}")
    return True if not is_bot else False


@app.error
def handle_errors(error):
    if isinstance(error, BoltUnhandledRequestError):
        logger.info(f"Intentionally unhandled request: {error}")
        return BoltResponse(status=200, body="")
    else:
        # other error patterns
        logger.debug(error)
        return BoltResponse(status=500, body="Something Wrong")


# tracks channel messages with non-bot user mentions
@app.message(re.compile('<@([UW][A-Za-z0-9]+)>'))
def handle_user_mention_message(context, message):
    if 'subtype' in message:
        logger.debug("Not a simple channel message: subtype exists")
        return
    else:
        logger.debug("Regular channel message: subtype is None")

    """Will likely need a switch here to handle an announcement in #general"""
    # context['matches'] creates a tuple with captured User IDs.
    # Transform tuple to set to remove potential duplicate User IDs
    raw_mentions_by_id = set(context['matches'])
    # Remove bot mentions just in case
    logger.debug(f"All mentioned users: {raw_mentions_by_id}")
    user_mentions_by_id = tuple(filter(isValidNonBotId, raw_mentions_by_id))
    logger.opt(colors=True).debug(f"Mentioned <red>human</red> users: {user_mentions_by_id}")
    channel_id = message['channel']
    message_ts = message['event_ts']
    two_days_in_unix_seconds = 3600 * 24 * 2
    remind_time = int(float(message_ts)) + two_days_in_unix_seconds
    remind_time = datetime.utcfromtimestamp(remind_time)
    with ps_pool.connection() as conn:
        conn.execute("""INSERT INTO
                     mention_message(channel_id, message_ts, remind_time)
                     VALUES(%s, %s, %s)""",
                     (channel_id, message_ts, remind_time))
        for user_id in user_mentions_by_id:
            conn.execute("""INSERT INTO
                         mention(channel_id, message_ts, user_id)
                         VALUES(%s, %s, %s)""",
                         (channel_id, message_ts, user_id))
        conn.commit()


# listens to reactions, but only does db query if it has mentions
@app.event("reaction_added")
def handle_reaction_for_mention_message(payload):
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
        # Only do db ops if there are mentions in the reaction message
        match = re.search(r"<@([UW][A-Za-z0-9]+)>", message['text'])
        if not match:
            logger.debug("react detected on non-mention msg, bail out event")
            return
    except SlackApiError as e:
        print(f"Error: {e}")
    logger.debug("react detected on mention mesage, begin db ops")
    user_id = payload['user']
    channel_id = payload['item']['channel']
    message_ts = payload['item']['ts']
    reaction = payload['reaction']
    logger.debug(f"user:{user_id} rxn:{reaction} chan_id:{channel_id} ts:{message_ts}")
    with ps_pool.connection() as conn:
        print(conn)
        logger.info(ps_pool.get_stats())

        # delete from mention using channel_id, message_ts, and user_id
        delete_query = conn.execute("""DELETE FROM mention
                        WHERE channel_id = %s
                        AND message_ts = %s
                        AND user_id = %s
                        RETURNING *""", (channel_id, message_ts, user_id))
        conn.commit()
        delete_row = delete_query.fetchone()
        if delete_row is None:
            logger.debug("An unmentioned user reacted on a mention message")
            return
        logger.debug(f"user:{delete_row[2]} responded to message:{delete_row[1]} in channel:{delete_row[0]}")

 
if __name__ == "__main__":
    logger.info(f"Startup at {datetime.now()}")
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"], ).start()
