import os
from discord.ext import commands
from bot_commands.general import General


class MyClient(commands.Bot):
    async def on_ready(self):
        print('Logged on as {0}!'.format(self.user))

# Prefix used to invoke the commands in the bot
client = MyClient(commands.when_mentioned_or('!'))
# Cog (Command Group) contains a group of commands to invoke separataley
client.add_cog(General(client))
# We're running the client with the specified token
client.run(os.getenv('DISCORD_TOKEN'))
