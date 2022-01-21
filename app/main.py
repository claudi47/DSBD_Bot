import os

from discord.ext import commands

from app.bot_commands.general import General
from app.kafka.consumers import init_consumers
from app.kafka.producers import init_producers


class MyClient(commands.Bot):
    consumers_producers_initialized = False

    async def on_ready(self):
        print('Logged on as {0}!'.format(self.user))
        if not self.consumers_producers_initialized:
            init_producers(self)
            init_consumers(self)
            self.consumers_producers_initialized = True


# Prefix used to invoke the commands in the bot
client = MyClient(commands.when_mentioned_or('!'))
# Cog (Command Group) contains a group of commands to invoke separataley
client.add_cog(General(client))
# We're running the client with the specified token
client.run(os.getenv('BOT_TOKEN_DSBD'))
