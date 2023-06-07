from celery import Celery
import os

app = Celery()
app.config_from_object('celeryconfig')


@app.task(serializer='json', name='etd-alma-service.tasks.send_to_alma')
def send_to_alma(message):
    print("message")
    print(message)
    new_message = {"hello": "from etd-alma-service"}
    app.send_task("tasks.tasks.do_task", args=[new_message], kwargs={},
                  queue=os.getenv('PUBLISH_QUEUE_NAME'))
