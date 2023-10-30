from celery import Celery
import os

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

arguments = {"hello": "world", "feature_flags": {
            'dash_feature_flag': "on",
            'alma_feature_flag': "on",
            'send_to_drs_feature_flag': "off",
            'drs_holding_record_feature_flag': "off"}}

res = app1.send_task('etd-alma-service.tasks.send_to_alma',
                     args=[arguments], kwargs={},
                     queue="etd_in_storage")
