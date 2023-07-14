from celery import Celery
import os
import logging
import etd
import json
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME

app = Celery()
app.config_from_object('celeryconfig')
etd.configure_logger()
logger = logging.getLogger('etd_alma')

FEATURE_FLAGS = "feature_flags"
ALMA_FEATURE_FLAG = "alma_feature_flag"
DASH_FEATURE_FLAG = "dash_feature_flag"

# tracing setup
JAEGER_NAME = os.getenv('JAEGER_NAME')
JAEGER_SERVICE_NAME = os.getenv('JAEGER_SERVICE_NAME')

resource = Resource(attributes={SERVICE_NAME: JAEGER_SERVICE_NAME})
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)

otlp_exporter = OTLPSpanExporter(endpoint=JAEGER_NAME, insecure=True)

span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)


@tracer.start_as_current_span("send_to_alma")
@app.task(serializer='json', name='etd-alma-service.tasks.send_to_alma')
def send_to_alma(json_message):
    logger.debug("message")
    logger.debug(json_message)
    current_span = trace.get_current_span()
    new_message = {"hello": "from etd-alma-service"}
    if FEATURE_FLAGS in json_message:
        feature_flags = json_message[FEATURE_FLAGS]
        new_message[FEATURE_FLAGS] = feature_flags
        if ALMA_FEATURE_FLAG in feature_flags and \
                feature_flags[ALMA_FEATURE_FLAG] == "on":
            if DASH_FEATURE_FLAG in feature_flags and \
                    feature_flags[DASH_FEATURE_FLAG] == "on":
                # Create Alma Record
                logger.debug("FEATURE IS ON>>>>>CREATE ALMA RECORD")
                current_span.add_event("FEATURE IS ON>>>>>CREATE ALMA RECORD")
            else:
                logger.debug("dash_feature_flag MUST BE ON FOR THE ALMA \
                    HOLDING TO BE CREATED. dash_feature_flag IS SET TO OFF")
                current_span.add_event("dash_feature_flag \
                    MUST BE ON FOR THE ALMA \
                    HOLDING TO BE CREATED. dash_feature_flag IS SET TO OFF")
        else:
            # Feature is off so do hello world
            logger.debug("FEATURE FLAGS FOUND")
            logger.debug(json_message[FEATURE_FLAGS])
            current_span.add_event("FEATURE FLAGS FOUND")
            current_span.add_event(json.dumps(json_message[FEATURE_FLAGS]))

    # If only unit testing, return the message and
    # do not trigger the next task.
    if "unit_test" in json_message:
        return new_message

    # publish to ingested_into_alma for helloworld,
    # eventually webhooks will do that instead
    current_span.add_event("to next queue")
    app.send_task("etd-alma-monitor-service.tasks.send_to_drs",
                  args=[new_message], kwargs={},
                  queue=os.getenv('PUBLISH_QUEUE_NAME'))
