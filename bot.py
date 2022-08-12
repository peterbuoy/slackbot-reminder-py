import os
import re
import psycopg_pool
from loguru import logger
from datetime import datetime
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
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

app = App(token=os.environ.get("SLACK_BOT_TOKEN"))
client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))


def isNotBotId(user_id: str):
    result = client.users_info(user=user_id)
    logger.debug(f"{result['user']['name']} is a bot ? {result['user']['is_bot']}")
    if not result['user']['is_bot']:
        return True
    return False


@app.message(re.compile('<@([UW][A-Za-z0-9]+)>'))
def create_reminder(context, message):
    # context['matches'] creates a tuple with captured User IDs.
    # Transform tuple to set to remove potential duplicate User IDs
    mentioned_users_by_id = set(context['matches'])
    # Remove bot mentions just in case
    logger.debug(f"""All mentioned users: {mentioned_users_by_id}""")
    mentioned_users_by_id = tuple(filter(isNotBotId, mentioned_users_by_id))
    logger.opt(colors=True).debug(f"""Mentioned <red>human</red> users: {mentioned_users_by_id}""")
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
        for user_id in mentioned_users_by_id:
            conn.execute("""INSERT INTO
                         mention(channel_id, message_ts, user_id)
                         VALUES(%s, %s, %s)""",
                         (channel_id, message_ts, user_id))
        conn.commit()


@app.event("reaction_added")
def track_reaction_for_mention_message(payload, say):
    # Retrieve message text from data in reaction payload
    # inclusive: oldest ts counted, oldest: only count ts after arg
    try:
        result = client.conversations_history(
                                            channel=payload['item']['channel'],
                                            inclusive=True,
                                            oldest=payload['item']['ts'],
                                            limit=1
                                        )
        message = result["messages"][0]
        # Print message text, check for mentions
        match = re.search(r"<@([UW][A-Za-z0-9]+)>", message['text'])
        if not match:
            logger.debug("react detected on non-mention msg, bail out event")
            return
    except SlackApiError as e:
        print(f"Error: {e}")
    logger.debug("react detected on mention mesage, begin db ops")
    user_id = payload['user']
    message_channel = payload['item']['channel']
    message_ts = payload['item']['ts']
    reaction = payload['reaction']
    logger.info(f"User{user_id} {reaction} {message_channel} {message_ts}")
    

if __name__ == "__main__":
    logger.info(f"Startup at {datetime.now()}")
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"], ).start()
