import asyncio
import datetime
import logging
import os
import urllib.parse
import uuid
from json import JSONDecodeError

import discord
import pytz
import requests
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

    return producers.user_auth_producer.produce(tx_id, UserAuthTransfer(user_id=ctx.author.id,
                                                                        username=ctx.author.name),
                                                headers={'channel_id': str(ctx.channel.id), 'web_site': website,
                                                         'category': category,
                                                         'junk_channel': '936928750967353414',
                                                         'author_id': str(ctx.author.id)})

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
        await ctx.send('Your operation is being processed, please wait!')

        tx_id = str(uuid.uuid4())
        advanced_scheduler.transaction_scheduler.add_job(self._operation_failed_message_send, 'date',
                                                         run_date=datetime.datetime.now(pytz.utc) + datetime.timedelta(
                                                             seconds=20),
                                                         args=[ctx.author.id, ctx.channel.id],
                                                         id=tx_id,
                                                         replace_existing=True,
                                                         misfire_grace_time=None)
        user_validation = _validation(ctx, 'goldbet', category, tx_id)
        search_entry_send = _search_entry_send(ctx, 'goldbet', category, tx_id)

        return

        try:
            user_validation_ack = await user_validation
            logging.info(f'User validation phase: message sent -- {user_validation_ack}')
        except KafkaException as exc:
            logging.error(f'User validation phase got an error: {exc}')
            user_validation.cancel()
        is_valid, description = await _validation(ctx, 'goldbet')
        if not is_valid:
            return await ctx.send(description)
        try:
            bet_data = goldbet.run(category)
        except JSONDecodeError as j:
            await ctx.send("Category temporarily disabled by the website, try another day")
            return print(j)
        search_data = _get_search_data(ctx, bet_data, 'goldbet')
        if search_data is not None:
            # sending json object to web server through POST method
            response_csv_filename = requests.post('http://server:8000/bot/goldbet/', json=search_data.data)
        else:
            return await ctx.send("There aren't any results in this category!")
        if not response_csv_filename.ok:
            await ctx.send("Error during the parsing of the file")
            print("Che è successo? Non è arrivato bene il file csv. LOL!")
        else:
            response_data = response_csv_filename.json()
            csv_file_path = '/tmpfiles/' + response_data['filename'].replace('"', '')
            with open(csv_file_path, "rb") as file:  # opening in read-binary mode
                # instance of discord File class that wants the filepointer and his new name (optional)
                discord_file = discord.File(file, f"goldbet_search_{ctx.author.name}_{category}_goldbet.csv")
                message = await ctx.send(f"Here's your research, {ctx.author.name}. Bet safely...", file=discord_file)
            # taking the url of the uploaded file on discord, saved into the CDN (Content Delivery Network)
            url_csv = message.attachments[0].url
            patching_associated_search_data = {'url_csv': url_csv, 'search_id': response_data['search_id']}
            patching_url_csv = requests.post('http://server:8000/bot/csv/', data=patching_associated_search_data)
            if not patching_url_csv.ok:
                await message.delete()

    @commands.command(brief='Shows the quotes of the principal soccer matches inside Bwin website',
                      description='This command shows the most important quotes about all the soccer matches inside bwin'
                                  ' website. The quotes are 1x2 and Over/Under')
    async def bwin(self, ctx: Context, category):
        await ctx.send('Your operation is being processed, please wait!')

        tx_id = str(uuid.uuid4())
        advanced_scheduler.transaction_scheduler.add_job(self._operation_failed_message_send, 'date',
                                                         run_date=datetime.datetime.now(pytz.utc) + datetime.timedelta(
                                                             seconds=20),
                                                         args=[ctx.author.id, ctx.channel.id],
                                                         id=tx_id,
                                                         replace_existing=True,
                                                         misfire_grace_time=None)
        user_validation = _validation(ctx, 'bwin', category, tx_id)
        search_entry_send = _search_entry_send(ctx, 'bwin', category, tx_id)
        return
        is_valid, description = await _validation(ctx, 'bwin')
        if not is_valid:
            return await ctx.send(description)
        bet_data = _call_retry(bwin.run, 2, category)
        search_data = _get_search_data(ctx, bet_data, 'bwin')
        if search_data is not None:
            response_csv_filename = requests.post('http://server:8000/bot/bwin/', json=search_data.data)
        else:
            return await ctx.send("There aren't any results in this category!")
        if not response_csv_filename.ok:
            await ctx.send("Error during the parsing of the file")
        else:
            response_data = response_csv_filename.json()
            csv_file_path = '/tmpfiles/' + response_data['filename'].replace('"', '')
            with open(csv_file_path, "rb") as file:  # opening in read-binary mode
                # instance of discord File class that wants the filepointer and his new name (optional)
                discord_file = discord.File(file, f"bwin_search_{ctx.author.name}_{category}_bwin.csv")
                message = await ctx.send(f"Here's your research, {ctx.author.name}. Bet safely...", file=discord_file)
            # taking the url of the uploaded file on discord, saved into the CDN (Content Delivery Network)
            url_csv = message.attachments[0].url
            patching_associated_search_data = {'url_csv': url_csv, 'search_id': response_data['search_id']}
            patching_url_csv = requests.post('http://server:8000/bot/csv/', data=patching_associated_search_data)
            if not patching_url_csv.ok:
                await message.delete()

        # await is a command similar to return but for async functions
        # await is necessary when a context switch happens (for example when two command are invoked simultaneously)
        # the funct stops and resumes his work after the other event finishes

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
                                          params={'band_period': args[1]})
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
                    result = requests.post('http://server:8000/bot/settings', params={'setting': 'toggle'},
                                           data={'web_site': args[0], 'state': args[1]})
                    if not result.ok:
                        return await ctx.send('Error during the toggle of the websites')
                    return
                except IndexError:
                    return await ctx.send('Error during the toggle of the websites: website and state are needed')
