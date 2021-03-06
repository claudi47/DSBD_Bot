import json
from json import JSONDecodeError

import asyncio
import datetime
import discord
import logging
import os
import pytz
import requests
import urllib.parse
import uuid
from confluent_kafka import KafkaException
from discord.ext import commands
from discord.ext.commands import Context

import app.kafka.producers as producers
from app.bet_models.search_data import SearchData
from app.bet_models.user_data import UserData
from ..bet_models.betting_data import BettingData
from ..models import UserAuthTransfer, SearchDataPartialInDb
from ..utils import advanced_scheduler
from ..web_sites_scripts import goldbet, bwin


# static functions are collocated outside the classes
def _get_search_data(ctx: Context, bet_data: BettingData, website):
    if bet_data is None:
        return None
    user_id = ctx.author.id
    username = ctx.author.name
    user_data = UserData(user_id, username)
    search_data = SearchData(bet_data, user_data, website)
    return search_data


def _validation(ctx: Context, website, category, tx_id) -> asyncio.Future:
    # can_execute = requests.get(f'http://server:8000/bot/validation/?user_id={ctx.author.id}&website={website}')
    return asyncio.gather(producers.user_auth_producer.produce(tx_id, UserAuthTransfer(user_id=ctx.author.id,
                                                                                username=ctx.author.name),
                                                        headers={'channel_id': str(ctx.channel.id), 'web_site': website,
                                                                 'category': category,
                                                                 'junk_channel': '936928750967353414',
                                                                 'author_id': str(ctx.author.id)}),
                   producers.user_limit_auth_producer.produce(tx_id, UserAuthTransfer(user_id=ctx.author.id,
                                                                                      username=ctx.author.name),
                                                              headers={'channel_id': str(ctx.channel.id),
                                                                       'web_site': website,
                                                                       'category': category,
                                                                       'junk_channel': '936928750967353414',
                                                                       'author_id': str(ctx.author.id)}))

    # if not can_execute.ok:
    #     return False, "Validation error!"
    # validation_data = can_execute.text
    # if 'banned' in validation_data:
    #     return False, "You got banned"
    # elif 'reached_max' in validation_data:
    #     return False, "You cannot do another research"
    # elif 'disabled' in validation_data:
    #     return False, "This website is temporarily disabled by the Admin"
    #
    # return True, ""


def _call_retry(function, retry_counter, *args):
    while retry_counter > 0:
        try:
            return function(*args)
        except Exception as ex:
            retry_counter -= 1
    return None


def _search_entry_send(ctx, web_site, category, tx_id) -> asyncio.Future:
    return producers.partial_search_entry_producer.produce(tx_id, SearchDataPartialInDb(web_site=web_site,
                                                                                        user_id=ctx.author.id),
                                                           headers={'channel_id': str(ctx.channel.id),
                                                                    'web_site': web_site,
                                                                    'category': category,
                                                                    'junk_channel': '936928750967353414',
                                                                    'author_id': str(ctx.author.id)})

class General(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def _operation_failed_message_send(self, user_id, channel_id):
        text_channel: discord.TextChannel = self.bot.get_channel(channel_id)
        await text_channel.send(f'<@{user_id}>, An error during the transaction occurred! Please, try again.')

        # Annotation to specify the decorator (Decorator Pattern)
    # The function command() contains another function called decorator(func) and returns it
    # During run-time, the method goldbet (or bwin) is passed to the decorator, through the command function,
    # who instantiates a Command class into a variable with the same name of the function passed to it
    @commands.command(brief='Shows the quotes of the principal soccer matches inside GoldBet website',
                      description='This command shows the most important quotes about all the soccer matches inside'
                                  ' GoldBet website. The quotes are 1x2 and Under/Over')
    # This is the function passed by param to the decorator -> decorator(func)
    async def goldbet(self, ctx: Context, category):
        if not os.path.exists('/usr/src/app/config/settings.in'):
            default_settings = {'goldbet': True, 'bwin': True}
            with open('/usr/src/app/config/settings.in', 'w') as settings_file:
                json.dump(default_settings, settings_file)

        with open('/usr/src/app/config/settings.in', 'r') as settings_file:
            settings = json.load(settings_file)

        if settings['goldbet'] is False:
            await ctx.send('This website has been disabled by the admin!')
            return

        await ctx.send('Your operation is being processed, please wait!')

        tx_id = str(uuid.uuid4())
        advanced_scheduler.transaction_scheduler.add_job(self._operation_failed_message_send, 'date',
                                                         run_date=datetime.datetime.now(pytz.utc) + datetime.timedelta(
                                                             seconds=20),
                                                         args=[ctx.author.id, ctx.channel.id],
                                                         id=tx_id,
                                                         replace_existing=True,
                                                         misfire_grace_time=None)
        _validation(ctx, 'goldbet', category, tx_id)
        _search_entry_send(ctx, 'goldbet', category, tx_id)

    @commands.command(brief='Shows the quotes of the principal soccer matches inside Bwin website',
                      description='This command shows the most important quotes about all the soccer matches inside bwin'
                                  ' website. The quotes are 1x2 and Over/Under')
    async def bwin(self, ctx: Context, category):
        if not os.path.exists('/usr/src/app/config/settings.in'):
            default_settings = {'goldbet': True, 'bwin': True}
            with open('/usr/src/app/config/settings.in', 'w') as settings_file:
                json.dump(default_settings, settings_file)

        with open('/usr/src/app/config/settings.in', 'r') as settings_file:
            settings = json.load(settings_file)

        if settings['bwin'] is False:
            await ctx.send('This website has been disabled by the admin!')
            return

        await ctx.send('Your operation is being processed, please wait!')

        tx_id = str(uuid.uuid4())
        advanced_scheduler.transaction_scheduler.add_job(self._operation_failed_message_send, 'date',
                                                         run_date=datetime.datetime.now(pytz.utc) + datetime.timedelta(
                                                             seconds=20),
                                                         args=[ctx.author.id, ctx.channel.id],
                                                         id=tx_id,
                                                         replace_existing=True,
                                                         misfire_grace_time=None)
        _validation(ctx, 'bwin', category, tx_id)
        _search_entry_send(ctx, 'bwin', category, tx_id)


    @commands.command()
    async def stat(self, ctx, stat: int):
        match stat:
            case 1:
                result = requests.get(f'http://{os.getenv("USER_SERVICE_URL")}/users/')
                response = result.json()
                await ctx.send(f"The number of users that used the bot is: {response['count']}\n"
                               f"They're: {', '.join(user['username'] for user in response['users'])}")
            case 2:
                result = requests.get(f'http://{os.getenv("BETDATA_SERVICE_URL")}/searches/')
                response = result.json()
                await ctx.send(f"The number of researches made by the users is: {response['total']}")
            case 3:
                result = requests.get(f'http://{os.getenv("BETDATA_SERVICE_URL")}/searches/')
                response = result.json()
                await ctx.send(f"The number of researches done in Goldbet website is: {response['goldbet']}\n"
                               f"The number of researches done in Bwin website is: {response['bwin']}")
            # case 4:
            #     response = result.json()
            #     message = ''
            #     for user in response['users']:
            #         message += f"{user['username']}, {user['count']}\n"
            #     await ctx.send(f"The average number of research for user is: {response['average']}\n"
            #                    "Research for each user: \n"
            #                    f"{message}")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def settings(self, ctx, type_of, *args):
        match type_of:
            case 'ban':
                try:
                    result = requests.put(f"http://{os.getenv('USER_SERVICE_URL')}/users/{urllib.parse.quote_plus(args[0])}/ban",
                                          params={'ban_period': args[1]})
                    if not result.ok:
                        return await ctx.send('Error during the suspension of the user')
                    return
                except IndexError:
                    return await ctx.send('Missing arguments: user and period are needed')
            case 'unban':
                try:
                    result = requests.put(f"http://{os.getenv('USER_SERVICE_URL')}/users/{urllib.parse.quote_plus(args[0])}/ban",
                                          params={'ban_period': 'null'})
                    if not result.ok:
                        return await ctx.send('Error during the unban of the user')
                    return
                except IndexError:
                    return await ctx.send('Missing arguments: user and period are needed')
            case 'max_searches':
                try:
                    result = requests.put(
                        f"http://{os.getenv('USER_SERVICE_URL')}/users/{urllib.parse.quote_plus(args[0])}/research-limit",
                        params={'limit': args[1]})
                    if not result.ok:
                        return await ctx.send('Error during the limit setting of researches for the user')
                    return
                except IndexError:
                    return await ctx.send('Missing arguments: user and limit are needed')
            case 'toggle':
                try:
                    if args[1] != 'enable' and args[1] != 'disable':
                        return await ctx.send('The state must be enable/disable')
                    if args[0] != 'bwin' and args[0] != 'goldbet':
                        return await ctx.send('The website must be goldbet/bwin')
                    if not os.path.exists('/usr/src/app/config/settings.in'):
                        default_settings = {'goldbet': True, 'bwin': True}
                        with open('/usr/src/app/config/settings.in', 'w') as settings_file:
                            json.dump(default_settings, settings_file)

                    with open('/usr/src/app/config/settings.in', 'r') as settings_file:
                        settings = json.load(settings_file)
                    settings[args[0]] = True if args[1] == 'enable' else False

                    with open('/usr/src/app/config/settings.in', 'w') as settings_file:
                        json.dump(settings, settings_file)
                except IndexError:
                    return await ctx.send('Error during the toggle of the websites: website and state are needed')
