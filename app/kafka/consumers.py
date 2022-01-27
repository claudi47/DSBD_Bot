import asyncio
import io
import logging
import threading
from abc import ABC, abstractmethod
from json import JSONDecodeError

import discord
from confluent_kafka import DeserializingConsumer, Consumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer
from discord.ext import commands

import app.kafka.producers as producers
from app.bot_commands.general import _call_retry
from app.models import UserAuthTransfer, UserAuthTransferReply, BetDataUpdateList
from app.web_sites_scripts import goldbet, bwin
import app.settings as config


class GenericConsumer(ABC):
    bootstrap_servers = config.broker_settings.broker

    @property
    @abstractmethod
    def group_id(self):
        ...

    @property
    @abstractmethod
    def auto_offset_reset(self):
        ...

    @property
    @abstractmethod
    def auto_commit(self):
        ...

    @property
    @abstractmethod
    def topic(self):
        ...

    @property
    @abstractmethod
    def schema(self):
        ...

    @abstractmethod
    def dict_to_model(self, map, ctx):
        ...

    def close(self):
        self._cancelled = True
        self._polling_thread.join()

    def consume_data(self):
        if not self._polling_thread.is_alive():
            self._polling_thread.start()

    @abstractmethod
    def _consume_data(self):
        ...

    def reset_state(self):
        self._cancelled = False

    def __init__(self, loop=None, client=None, normal_consumer=False):
        if not normal_consumer:
            json_deserializer = JSONDeserializer(self.schema,
                                                 from_dict=self.dict_to_model)
            string_deserializer = StringDeserializer('utf_8')

            consumer_conf = {'bootstrap.servers': self.bootstrap_servers,
                             'key.deserializer': string_deserializer,
                             'value.deserializer': json_deserializer,
                             'group.id': self.group_id,
                             'auto.offset.reset': self.auto_offset_reset,
                             'enable.auto.commit': self.auto_commit,
                             'allow.auto.create.topics': True}
        else:
            consumer_conf = {'bootstrap.servers': self.bootstrap_servers,
                             'group.id': self.group_id,
                             'auto.offset.reset': self.auto_offset_reset,
                             'enable.auto.commit': self.auto_commit,
                             'allow.auto.create.topics': True}

        self._loop = loop or asyncio.get_event_loop()
        if not normal_consumer:
            self._consumer = DeserializingConsumer(consumer_conf)
        else:
            self._consumer = Consumer(consumer_conf)
        self._cancelled = False
        self._consumer.subscribe([self.topic])
        self._polling_thread = threading.Thread(target=self._consume_data)
        self.client: commands.Bot = client


class UserAuthConsumer(GenericConsumer):

    @property
    def group_id(self):
        return 'my_group'

    @property
    def auto_offset_reset(self):
        return 'earliest'

    @property
    def auto_commit(self):
        return False

    @property
    def topic(self):
        return 'user-auth-reply'

    @property
    def schema(self):
        return """{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "User Auth reply",
  "description": "User Auth kafka reply",
  "type": "object",
  "properties": {
    "username": {
      "description": "Discord username",
      "type": "string"
    },
    "user_id": {
      "description": "Discord User identifier",
      "type": "string"
    },
    "authorized": {
      "description": "Whether the user is authorized",
      "type": "boolean"
    }
  }
}"""

    def dict_to_model(self, map, ctx):
        if map is None:
            return None

        return UserAuthTransferReply(**map)

    def parse_website(self, msg):
        web_site = msg.headers()[1][1].decode()
        if web_site == 'goldbet':
            try:
                return goldbet.run(msg.headers()[2][1].decode())
            except JSONDecodeError as j:
                asyncio.run_coroutine_threadsafe(
                    self.client.get_channel(int(msg.headers()[0][1])).send('Website temporarily disabled'),
                    loop=self._loop)
                return print(j)
        elif web_site == 'bwin':
            return _call_retry(bwin.run, 2, msg.headers()[2][1].decode())

    def _consume_data(self):
        while not self._cancelled:
            try:
                msg = self._consumer.poll(0.1)
                if msg is None:
                    continue

                # headers: [0] channel_id, [1] web_site, [2] category
                user_auth: UserAuthTransferReply = msg.value()
                if user_auth is not None:
                    if user_auth.authorized:
                        bet_data = self.parse_website(msg)
                        if bet_data is None:
                            asyncio.run_coroutine_threadsafe(
                                self.client.get_channel(int(msg.headers()[0][1])).send('Website temporarily disabled'),
                                loop=self._loop)
                            raise Exception('Parsing error')

                        async def get_csvgen_ack(csv_gen_ack_fut: asyncio.Future):
                            try:
                                await asyncio.wait_for(csv_gen_ack_fut, 20)
                            except:
                                self.client.get_channel(int(msg.headers()[0][1])).send('Transaction error (ack)')

                        producers_acks = asyncio.gather(producers.csv_gen_producer.produce(msg.key(),
                                                                                           BetDataUpdateList.parse_obj(
                                                                                               bet_data.data),
                                                                                           msg.headers()),
                                                        producers.bet_data_apply_producer.produce(msg.key(),
                                                                                                  BetDataUpdateList.parse_obj(
                                                                                                      bet_data.data),
                                                                                                  headers=msg.headers()))
                        asyncio.run_coroutine_threadsafe(get_csvgen_ack(producers_acks), loop=self._loop)
                    else:
                        asyncio.run_coroutine_threadsafe(
                            self.client.get_channel(int(msg.headers()[0][1])).send(
                                'Transaction error! User not authorized!'),
                            loop=self._loop)

                    self._consumer.commit(msg)
                else:
                    logging.warning(f'Null value for the message: {msg.key()}')
                    self._consumer.commit(msg)
            except Exception as exc:
                logging.error(exc)
                try:
                    self._consumer.commit(msg)
                except:
                    pass

                # break

        self._consumer.close()


class UserAuthConsumerNormal(GenericConsumer):

    @property
    def group_id(self):
        return 'my_group'

    @property
    def auto_offset_reset(self):
        return 'earliest'

    @property
    def auto_commit(self):
        return False

    @property
    def topic(self):
        return 'user_auth'

    @property
    def schema(self):
        return """{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "User Auth reply",
  "description": "User Auth kafka reply",
  "type": "object",
  "properties": {
    "username": {
      "description": "Discord username",
      "type": "string"
    },
    "user_id": {
      "description": "Discord User identifier",
      "type": "string"
    }
  }
}"""

    def dict_to_model(self, map, ctx):
        if map is None:
            return None

        return UserAuthTransfer(**map)

    def parse_website(self):
        pass

    def _consume_data(self):
        while not self._cancelled:
            try:
                msg = self._consumer.poll(0.1)
                if msg is None:
                    continue

                user_auth: UserAuthTransfer = msg.value()
                if user_auth is not None:
                    print(f'headers: {msg.headers()}')
                    self._consumer.commit(msg)
                else:
                    logging.warning(f'Null value for the message: {msg.key()}')
                    self._consumer.commit(msg)
            except Exception as exc:
                logging.error(exc)
                try:
                    self._consumer.commit(msg)
                except:
                    pass

                # break

        self._consumer.close()


class CsvGenReplyConsumer(GenericConsumer):

    @property
    def group_id(self):
        return 'my_group'

    @property
    def auto_offset_reset(self):
        return 'earliest'

    @property
    def auto_commit(self):
        return False

    @property
    def topic(self):
        return 'csv-gen-reply'

    @property
    def schema(self):
        return None

    def dict_to_model(self, map, ctx):
        return None

    def _consume_data(self):
        while not self._cancelled:
            try:
                msg = self._consumer.poll(0.1)
                if msg is None:
                    continue

                csv_str = msg.value()
                if csv_str is not None:
                    print('CSV gen reply!')
                    print(f'headers: {msg.headers()}')
                    with io.BytesIO(msg.value()) as csv_bytes:
                        res: discord.Message = asyncio.run_coroutine_threadsafe(
                            self.client.get_channel(int(msg.headers()[0][1])).send('Here is the CSV file',
                                                                                   file=discord.File(csv_bytes,
                                                                                                     filename='csv.txt')),
                            self._loop).result(20)

                    async def final_transaction_step():
                        await producers.bet_data_finish_producer.produce(msg.key(), res.attachments[0].url,
                                                                         headers=msg.headers())

                    asyncio.run_coroutine_threadsafe(final_transaction_step(), self._loop).result(20)
                    self._consumer.commit(msg)
                else:
                    logging.warning(f'Null value for the message: {msg.key()}')
                    self._consumer.commit(msg)
            except Exception as exc:
                logging.error(exc)
                try:
                    self._consumer.commit(msg)
                    asyncio.run_coroutine_threadsafe(
                        self.client.get_channel(msg.headers()[0][1]).send('Transaction error! Final step'), self._loop)
                except:
                    pass

                # break

        self._consumer.close()


user_auth_consumer: UserAuthConsumer
csv_gen_reply_consumer: CsvGenReplyConsumer


def init_consumers(client=None):
    global user_auth_consumer, csv_gen_reply_consumer

    user_auth_consumer = UserAuthConsumer(asyncio.get_running_loop(), client)
    csv_gen_reply_consumer = CsvGenReplyConsumer(asyncio.get_running_loop(), client, normal_consumer=True)
    user_auth_consumer.consume_data()
    csv_gen_reply_consumer.consume_data()


def close_consumers():
    user_auth_consumer.close()
    csv_gen_reply_consumer.close()
