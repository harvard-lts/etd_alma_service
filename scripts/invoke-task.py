from celery import Celery
import os

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

arguments = {"hello": "world", "feature_flags": {
            'dash_feature_flag': os.getenv("DASH_FEATURE_FLAG"),
            'alma_feature_flag': os.getenv("ALMA_FEATURE_FLAG"),
            'send_to_drs_feature_flag':
            os.getenv("SEND_TO_DRS_FEATURE_FLAG")}}

res = app1.send_task('etd-alma-service.tasks.send_to_alma',
                     args=[arguments], kwargs={},
                     queue=os.getenv("CONSUME_QUEUE_NAME"))
