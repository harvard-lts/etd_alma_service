from celery import Celery
import os
import logging
import etd

app = Celery()
app.config_from_object('celeryconfig')
etd.configure_logger()
logger = logging.getLogger('etd_alma')


@app.task(serializer='json', name='etd-alma-service.tasks.send_to_alma')
def send_to_alma(message):
    logger.info("message")
    logger.info(message)
    new_message = {"hello": "from etd-alma-service"}
    # publish to ingested_into_alma for helloworld,
    # eventually webhooks will do that instead
    app.send_task("etd-alma-monitor-service.tasks.send_to_drs",
                  args=[new_message], kwargs={},
                  queue=os.getenv('PUBLISH_QUEUE_NAME'))
