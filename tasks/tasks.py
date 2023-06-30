from celery import Celery
import os
import logging
import etd
import json

app = Celery()
app.config_from_object('celeryconfig')
etd.configure_logger()
logger = logging.getLogger('etd_alma')


@app.task(serializer='json', name='etd-alma-service.tasks.send_to_alma')
def send_to_alma(message):
    logger.info("message")
    logger.info(message)
    new_message = {"hello": "from etd-alma-service"}
    if "feature_flags" in json_message:
        feature_flags = json_message["feature_flags"]
        new_message["feature_flags"] = feature_flags
        if "alma_feature_flag" in feature_flags and \
                feature_flags["alma_feature_flag"] == "on":
            if "dash_feature_flag" in feature_flags and \
                    feature_flags["dash_feature_flag"] == "on":
                # Create Alma Record
                print("FEATURE IS ON>>>>>CREATE ALMA RECORD")
            else:
                print("dash_feature_flag MUST BE ON FOR THE ALMA \
                    HOLDING TO BE CREATED. dash_feature_flag IS SET TO OFF")
        else:
            # Feature is off so do hello world
            print("FEATURE FLAGS FOUND")
            print(json_message['feature_flags'])

    # publish to ingested_into_alma for helloworld,
    # eventually webhooks will do that instead
    app.send_task("etd-alma-monitor-service.tasks.send_to_drs",
                  args=[new_message], kwargs={},
                  queue=os.getenv('PUBLISH_QUEUE_NAME'))
