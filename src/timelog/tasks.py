from __future__ import absolute_import

from celery import shared_task
from django.conf import settings
from django.core.cache import cache
from django.utils import timezone
from timelog.lib import analyze_log_file, PATTERN
from boto3 import Session
from decimal import Decimal
import numpy as np

import logging
logger = logging.getLogger(__name__)

@shared_task
def push_timelog_analytics_to_cloudwatch():
    if not hasattr(settings, 'AWS_ACCESS_KEY_ID') or not settings.AWS_ACCESS_KEY_ID \
    or not hasattr(settings, 'AWS_SECRET_ACCESS_KEY') or not settings.AWS_SECRET_ACCESS_KEY \
    or not hasattr(settings, 'AWS_REGION') or not settings.AWS_REGION:
        return
    
    if not hasattr(settings, 'TIMELOG_LOG')or not settings.TIMELOG_LOG:
        return
    
    if not hasattr(settings, 'CLOUDWATCH_NAMESPACE')or not settings.CLOUDWATCH_NAMESPACE \
    or not hasattr(settings, 'CLOUDWATCH_ENV')or not settings.CLOUDWATCH_ENV \
    or not hasattr(settings, 'CLOUDWATCH_SERVER')or not settings.CLOUDWATCH_SERVER:
        return
    
    acquire_lock = lambda: cache.add('CLOUDWATCH_TIMELOG_RUNNING', 'true', None)
    release_lock = lambda: cache.delete('CLOUDWATCH_TIMELOG_RUNNING')
    
    if acquire_lock():
        try:
            last_runtime = cache.get('CLOUDWATCH_TIMELOG_LAST_RUNTIME', None)
            data = analyze_log_file(settings.TIMELOG_LOG, PATTERN, progress=False, start_at=last_runtime)
            
            average_load_time = Decimal('-1') # if no activity on the site, we report -1 as average load time by default.
            max_load_time = Decimal('-1') # if no activity on the site, we report -1 as max load time by default.
            if data:
                load_times = np.array([ item['times'] for item in data.values() ])
                load_times = np.hstack(load_times.flat) # flatten the array
                average_load_time = Decimal(np.average(load_times)).quantize(Decimal('0.001'))
                max_load_time = Decimal(np.max(load_times)).quantize(Decimal('0.001'))
            
            aws = Session(aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
                          aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY, 
                          region_name=settings.AWS_REGION)
            cloudwatch = aws.client('cloudwatch')
            metric_data = [{
                'MetricName':'AveragePageResponseTime',
                'Dimensions':[{
                   'Name':'Env', 
                   'Value':settings.CLOUDWATCH_ENV
                },{
                   'Name':'Server', 
                   'Value':settings.CLOUDWATCH_SERVER
                }],
                'Timestamp':timezone.now(),
                'Value':average_load_time,
                'Unit': 'Seconds'
            },{
                'MetricName':'MaxPageResponseTime',
                'Dimensions':[{
                   'Name':'Env', 
                   'Value':settings.CLOUDWATCH_ENV
                },{
                   'Name':'Server', 
                   'Value':settings.CLOUDWATCH_SERVER
                }],
                'Timestamp':timezone.now(),
                'Value':max_load_time,
                'Unit': 'Seconds'
            }]
            cloudwatch.put_metric_data(Namespace=settings.CLOUDWATCH_NAMESPACE,MetricData=metric_data)
            
            cache.add('CLOUDWATCH_TIMELOG_LAST_RUNTIME', 0, None) # create key if not exists. default to 0
            cache.set('CLOUDWATCH_TIMELOG_LAST_RUNTIME', timezone.now())
        except IOError:
            logger.warn("Timelog file {} not found".format(settings.TIMELOG_LOG))
        except Exception:
            logger.exception("Error pushing timelog analytics to cloudwatch")
        finally:
            release_lock()