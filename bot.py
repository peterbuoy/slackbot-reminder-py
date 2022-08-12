import os
import re
import psycopg_pool
from datetime import datetime
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
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
print(ps_pool.get_stats())

app = App(token=os.environ.get("SLACK_BOT_TOKEN"))


@app.message(re.compile('<@([UW][A-Za-z0-9]+)>'))
def create_reminder(context, message, say):
    # context['matches'] creates a tuple with captured User IDs.
    # Transform tuple to set to remove potential duplicate User IDs
    mentioned_users_by_id = set(context['matches'])
    # people can mention bots. This is bad!
    channel_id = message['channel']
    message_ts = message['event_ts']
    two_days_in_unix_seconds = 3600 * 24
    print(message)
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
def track_reaction(payload, say):
    print(f"\nREACTION PAYLOAD: {payload}")
    user_id = payload['user']
    message_channel = payload['item']['channel']
    message_ts = payload['item']['ts']
    reaction = payload['reaction']
    print(f"{user_id} {reaction} {message_channel} {message_ts} ")
    say(f"""User <@{user_id}> added a :{reaction}:
    to a message in the channel <#{message_channel}>""")
    # use this info to remove the reference in the mention table


if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
