# slackbot-reminder-py
A Slackbot written in Bolt for Python that reminds users to respond to mentions within a timely manner.

# Setting up Poetry 
1. Install Poetry using version 1.1.13
2. Run `poetry install` to update packages

# How to start the bot with PM2
1. Set `ENV` environment variable to `dev` or `prod`
    * Example: `export ENV=prod`
2. Run `pm2 start 'poetry run python3 bot.py' --name slack-bot-reminder` to start the bot via PM2
