import os
import json
import logging
import uuid
import random
import redis
import requests
from confluent_kafka import Producer
from flask import Flask, jsonify, request, make_response

# OpenTelemetry Instrumentation
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
RequestsInstrumentor().instrument()
RedisInstrumentor().instrument()
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
from opentelemetry.trace import get_tracer_provider
inst = ConfluentKafkaInstrumentor()
tracer_provider = get_tracer_provider()
# End of OpenTelemetry Instrumentation

# System Performance
from opentelemetry.metrics import set_meter_provider
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
exporter = ConsoleMetricExporter()
set_meter_provider(MeterProvider([PeriodicExportingMetricReader(exporter)]))
SystemMetricsInstrumentor().instrument()
configuration = {
    "system.memory.usage": ["used", "free", "cached"],
    "system.cpu.time": ["idle", "user", "system", "irq"],
    "system.network.io": ["transmit", "receive"],
    "process.memory.usage": None,
    "process.memory.virtual": None,
    "process.cpu.time": ["user", "system"],
    "process.context_switches": ["involuntary", "voluntary"],
}
# end of System Performance

log_level = os.getenv("APP_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, log_level, logging.INFO))
logging.getLogger('werkzeug').setLevel(getattr(logging, log_level, logging.INFO))
app = Flask(__name__)
redis_client = None
INTERNAL_ERROR = "INTERNAL SERVER ERROR"

# Custom Metric:
from opentelemetry.metrics import get_meter_provider
meter = get_meter_provider().get_meter("api.authentication.metrics")
unauthorized_counter = meter.create_counter(
    name="unauthorized_user_count",
    description="Number of unauthorized authentication attempts",
    unit="1"
)


def get_env_variable(var_name, default=None):
    value = os.environ.get(var_name)
    if value is None:
        if default is not None:
            return default
        else:
            raise ValueError(f"Environment variable '{var_name}' not set.")
    return value


def request_log(component: str, payload:dict = None ):
    transaction_id = str(uuid.uuid4())
    request_message = {
        'message': 'Request',
        'component': component,
        'transactionId': transaction_id
    }
    if payload:
        request_message['payload'] = payload
    logging.info(request_message)
    return transaction_id


def response_log(transaction_id:str, component: str, return_code, payload:dict = None):
    response_message = {
        'message': 'Response',
        'component': component,
        'transactionId': transaction_id,
        'returnCode': return_code
    }
    if payload:
        response_message['payload'] = payload
    logging.info(response_message)


def publish_to_kafka(transaction_id: str, user: dict, message: str):
    kafka_server = get_env_variable("KAFKA_SERVER")
    conf = {
        'bootstrap.servers': kafka_server
    }
    producer = Producer(conf)
    producer = inst.instrument_producer(producer, tracer_provider)
    topic = "test"

    def delivery_report(err, _):
        if err is not None:
            logging.error(f"Error publishing to Kafka: {err}")
        else:
            logging.info("Published to Kafka")
    message = {
        "id": transaction_id,
        "message": message,
        "user": user
    }
    producer.produce(topic, value=str(message), callback=delivery_report)
    producer.poll(0)
    producer.flush()


def load_redis(user_data: dict) -> None:
    global redis_client
    if not redis_client:
        redis_host = get_env_variable("REDIS_HOST")
        redis_client = redis.Redis(host=redis_host, port=6379, decode_responses=True)
    redis_client.set(user_data['userId'], json.dumps(user_data))


def check_redis(user_id: str) -> dict:
    global redis_client
    if not redis_client:
        redis_host = get_env_variable("REDIS_HOST")
        redis_client = redis.Redis(host=redis_host, port=6379, decode_responses=True)
    return redis_client.get(user_id)


def get_member(user_id: str) -> dict:
    user_data = None
    payload = {
        'userId': user_id,
    }
    url = get_env_variable("MEMBER_MANAGEMENT_URL")
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        user_data = response.json()
    return user_data


def authenticate_user(transaction_id: str, user_id: str):
    # Random chance of 1 in 20 of not being authenticated
    if random.randint(1, 20) == 1:
        user = {
            "userId": user_id
        }
        publish_to_kafka(transaction_id, user, "Unauthorized")
        unauthorized_counter.add(1, {"userId": user_id})
        return None

    user_data = check_redis(user_id)
    if not user_data:
        user_data = get_member(user_id)
        if user_data:
            load_redis(user_data)
    return user_data


@app.route("/authenticate", methods=["POST"])
def authenticate():
    return_code = 200
    component = 'authenticate'
    transaction_id = None
    try:
        data = request.get_json()
        user_id = data.get("userId", None)
        payload = {
            'userId': user_id,
        }
        transaction_id = request_log(component, payload)
        if not user_id:
            return_code = 400
        else:
            user = authenticate_user(transaction_id, user_id)
            if not user:
                return_code = 401
            else:
                payload = user
    except Exception as ex:
        return_code = 500
        payload = {"error": INTERNAL_ERROR, "details": str(ex)}
    response_log(transaction_id, component, return_code, payload)
    return make_response(jsonify(payload), return_code)
