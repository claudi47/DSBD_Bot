import asyncio
import threading
from abc import abstractmethod, ABC

from confluent_kafka import SerializingProducer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer

from app.models import SearchDataPartialInDb, UserAuthTransfer, BetDataUpdateList
import app.settings as config
from app.utils.advanced_scheduler import async_repeat_deco


class GenericProducer(ABC):
    bootstrap_servers = config.broker_settings.broker
    schema_registry_conf = {'url': config.broker_settings.schema_registry}

    # bootstrap_servers = 'localhost:9092'
    # schema_registry_conf = {'url': 'http://localhost:8081'}

    @abstractmethod
    def model_to_dict(self, obj, ctx):
        ...

    @property
    @abstractmethod
    def schema(self):
        ...

    @abstractmethod
    def produce(self, id, value, headers) -> asyncio.Future:
        ...

    def _produce_data(self):
        while not self._cancelled:
            self._producer.poll(0.1)

    def produce_data(self):
        if not self._polling_thread.is_alive():
            self._polling_thread.start()

    def close(self):
        self._cancelled = True
        self._polling_thread.join()

    def reset_state(self):
        self._cancelled = False

    def __init__(self, loop=None, client=None, normal=False):
        if not normal:
            schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)

            json_serializer = JSONSerializer(self.schema, schema_registry_client, to_dict=self.model_to_dict)

            producer_conf = {'bootstrap.servers': self.bootstrap_servers,
                             'key.serializer': StringSerializer('utf_8'),
                             'value.serializer': json_serializer
                             }
            self._producer = SerializingProducer(producer_conf)
        else:
            producer_conf = {'bootstrap.servers': self.bootstrap_servers}
            self._producer = SerializingProducer(producer_conf)

        self._loop = loop or asyncio.get_event_loop()

        self._polling_thread = threading.Thread(target=self._produce_data)
        self._cancelled = False
        self.client = client


class PartialSearchEntryProducer(GenericProducer):
    topic = 'search-entry'

    def model_to_dict(self, obj: SearchDataPartialInDb, ctx):
        if obj is None:
            return None

        return obj.dict(exclude={'id'})

    @property
    def schema(self):
        return """{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Partial Search data",
  "description": "Partial search data",
  "type": "object",
  "properties": {
    "web_site": {
      "description": "Website name",
      "type": "string"
    },
    "user_id": {
      "description": "User's Discord id",
      "type": "string"
    }
  },
  "required": [
    "web_site",
    "user_id"
  ]
}"""

    def produce(self, id, value, headers=None):
        result_fut = self._loop.create_future()

        def delivery_report(err, msg):
            """ Called once for each message produced to indicate delivery result.
                Triggered by poll() or flush(). """
            if err is not None:
                print('Message delivery failed: {}'.format(err))
                self._loop.call_soon_threadsafe(result_fut.set_exception, KafkaException(err))
            else:
                print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
                self._loop.call_soon_threadsafe(result_fut.set_result, msg)

        self._producer.produce(topic=self.topic, key=id, value=value, on_delivery=delivery_report, headers=headers)
        return result_fut


class UserAuthProducer(GenericProducer):
    topic = 'user-auth'

    def model_to_dict(self, obj: UserAuthTransfer, ctx):
        if obj is None:
            return None

        return obj.dict()

    @property
    def schema(self):
        return """{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "User Auth Request",
  "description": "User Auth request data",
  "type": "object",
  "properties": {
    "user_id": {
      "description": "User's Discord id",
      "type": "string"
    },
    "username": {
      "description": "User's nick",
      "type": "string"
    }
  },
  "required": [
    "user_id",
    "username"
  ]
}"""

    def produce(self, id, value, headers=None):
        result_fut = self._loop.create_future()

        def delivery_report(err, msg):
            """ Called once for each message produced to indicate delivery result.
                Triggered by poll() or flush(). """
            if err is not None:
                print('Message delivery failed: {}'.format(err))
                self._loop.call_soon_threadsafe(result_fut.set_exception, KafkaException(err))
            else:
                print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
                self._loop.call_soon_threadsafe(result_fut.set_result, msg)

        self._producer.produce(topic=self.topic, key=id, value=value, on_delivery=delivery_report, headers=headers)
        return result_fut


class CsvGenProducer(GenericProducer):
    topic = 'csv-gen'

    def model_to_dict(self, obj: BetDataUpdateList, ctx):
        if obj is None:
            return None

        return obj.dict(exclude={'data': {'__all__': {'timestamp'}}})

    @property
    def schema(self):
        return """{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "CSV Generation Request",
  "description": "CSV Generation Kafka Request",
  "type": "object",
  "properties": {
    "data": {
      "description": "Bet Data",
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "date": {
            "type": "string"
          },
          "match": {
            "type": "string"
          },
          "one": {
            "type": "string"
          },
          "ics": {
            "type": "string"
          },
          "two": {
            "type": "string"
          },
          "gol": {
            "type": "string"
          },
          "over": {
            "type": "string"
          },
          "under": {
            "type": "string"
          }
        }
      }
    }
  }
}"""

    def produce(self, id, value, headers=None):
        result_fut = self._loop.create_future()

        def delivery_report(err, msg):
            """ Called once for each message produced to indicate delivery result.
                Triggered by poll() or flush(). """
            if err is not None:
                print('Message delivery failed: {}'.format(err))
                self._loop.call_soon_threadsafe(result_fut.set_exception, KafkaException(err))
            else:
                print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
                self._loop.call_soon_threadsafe(result_fut.set_result, msg)

        self._producer.produce(topic=self.topic, key=id, value=value, on_delivery=delivery_report, headers=headers)
        return result_fut


class BetDataProducer(CsvGenProducer):
    topic = 'bet-data-apply'


class BetDataFinishProducer(GenericProducer):
    topic = 'bet-data-finish'

    def model_to_dict(self, obj, ctx):
        return None

    @property
    def schema(self):
        return None

    def produce(self, id, value, headers) -> asyncio.Future:
        result_fut = self._loop.create_future()

        def delivery_report(err, msg):
            """ Called once for each message produced to indicate delivery result.
                Triggered by poll() or flush(). """
            if err is not None:
                print('Message delivery failed: {}'.format(err))
                self._loop.call_soon_threadsafe(result_fut.set_exception, KafkaException(err))
            else:
                print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
                self._loop.call_soon_threadsafe(result_fut.set_result, msg)

        self._producer.produce(topic=self.topic, key=id, value=value, on_delivery=delivery_report, headers=headers)
        return result_fut

class UserLimitPreliminary(GenericProducer):
    topic = 'user-limit-auth'

    def model_to_dict(self, obj: UserAuthTransfer, ctx):
        if obj is None:
            return None

        return obj.dict()

    @property
    def schema(self):
        return """{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "User Auth Request",
  "description": "User Auth request data",
  "type": "object",
  "properties": {
    "user_id": {
      "description": "User's Discord id",
      "type": "string"
    },
    "username": {
      "description": "User's nick",
      "type": "string"
    }
  },
  "required": [
    "user_id",
    "username"
  ]
}"""

    def produce(self, id, value, headers) -> asyncio.Future:
        result_fut = self._loop.create_future()

        def delivery_report(err, msg):
            """ Called once for each message produced to indicate delivery result.
                Triggered by poll() or flush(). """
            if err is not None:
                print('Message delivery failed: {}'.format(err))
                self._loop.call_soon_threadsafe(result_fut.set_exception, KafkaException(err))
            else:
                print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
                self._loop.call_soon_threadsafe(result_fut.set_result, msg)

        self._producer.produce(topic=self.topic, key=id, value=value, on_delivery=delivery_report, headers=headers)
        return result_fut


partial_search_entry_producer: PartialSearchEntryProducer
user_auth_producer: UserAuthProducer
csv_gen_producer: CsvGenProducer
bet_data_apply_producer: BetDataProducer
bet_data_finish_producer: BetDataFinishProducer
user_limit_auth_producer: UserLimitPreliminary


def init_producers(client=None):
    @async_repeat_deco(3, 3, always_reschedule=True)
    async def init_partial_search_entry_producer(_):
        global partial_search_entry_producer
        partial_search_entry_producer = PartialSearchEntryProducer(asyncio.get_running_loop(), client)
        partial_search_entry_producer.produce_data()

    @async_repeat_deco(3, 3, always_reschedule=True)
    async def init_user_auth_producer(_):
        global user_auth_producer
        user_auth_producer = UserAuthProducer(asyncio.get_running_loop(), client)
        user_auth_producer.produce_data()

    @async_repeat_deco(3, 3, always_reschedule=True)
    async def init_csv_gen_producer(_):
        global csv_gen_producer
        csv_gen_producer = CsvGenProducer(asyncio.get_running_loop(), client)
        csv_gen_producer.produce_data()

    @async_repeat_deco(3, 3, always_reschedule=True)
    async def init_betdata_apply_producer(_):
        global bet_data_apply_producer
        bet_data_apply_producer = BetDataProducer(asyncio.get_running_loop(), client)
        bet_data_apply_producer.produce_data()

    @async_repeat_deco(3, 3, always_reschedule=True)
    async def init_betdata_finish_producer(_):
        global bet_data_finish_producer
        bet_data_finish_producer = BetDataFinishProducer(asyncio.get_running_loop(), client, normal=True)
        bet_data_finish_producer.produce_data()

    @async_repeat_deco(3, 3, always_reschedule=True)
    async def init_user_limit_auth_producer(_):
        global user_limit_auth_producer
        user_limit_auth_producer = UserLimitPreliminary(asyncio.get_running_loop(), client)
        user_limit_auth_producer.produce_data()

    asyncio.run_coroutine_threadsafe(init_partial_search_entry_producer('partial_search_entry_producer'), loop=asyncio.get_running_loop())
    asyncio.run_coroutine_threadsafe(init_user_auth_producer('user_auth_producer'), loop=asyncio.get_running_loop())
    asyncio.run_coroutine_threadsafe(init_csv_gen_producer('csg_gen_producer'), loop=asyncio.get_running_loop())
    asyncio.run_coroutine_threadsafe(init_betdata_apply_producer('betdata_apply_producer'), loop=asyncio.get_running_loop())
    asyncio.run_coroutine_threadsafe(init_betdata_finish_producer('betdata_finish_producer'), loop=asyncio.get_running_loop())
    asyncio.run_coroutine_threadsafe(init_user_limit_auth_producer('user_limit_auth_producer'), loop=asyncio.get_running_loop())


def close_producers():
    partial_search_entry_producer.close()
    user_auth_producer.close()
    csv_gen_producer.close()
    bet_data_finish_producer.close()
    user_limit_auth_producer.close()
