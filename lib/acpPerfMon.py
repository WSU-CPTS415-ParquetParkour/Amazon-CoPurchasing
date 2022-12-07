#! /usr/bin/python3

import os
import re
import time
import configparser as cfg
from datetime import datetime
from collections import Counter

project_root = re.sub('(?<=Amazon-CoPurchasing).*', '', os.path.abspath('.'))
config_path = os.path.join(project_root, 'etc', 'config.ini')


class PerfMon:
    def __init__(self, caller_name):
        # List of tuples in the form [(timestamp, action)] (JR)
        self.timelog = []
        # Counter() stores values as a dict in the form {'item': n}
        self.counter = Counter()
        self.measured_fn_name = caller_name

    def add_timelog_event(self, action):
        self.timelog += [(time.perf_counter(), action)]

    def increment_counter(self, event):
        self.counter[event] += 1

    def get_all(self):
        return {'timelog': self.timelog, 'event counters': self.counter}

    def summarise(self):
        total_time = self.timelog[-1][0]-self.timelog[0][0]
        total_ops = sum([x for x in self.counter.values()])
        summary = {
            'total duration': f'{total_time:0.4f}',
            'average op time': f'{total_time/len(self.timelog):0.4f}',
            'total ops': f'{total_ops}'
        }
        return summary
    
    def log_all(self):
        datestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = os.path.join(project_root, 'var', 'perf')
        summary = self.summarise()

        timelog_path = os.path.join(output_path, '%(datestamp)s_timelog_%(caller)s.csv' % {'caller':self.measured_fn_name, 'datestamp':datestamp})
        counts_path = os.path.join(output_path, '%(datestamp)s_counts_%(caller)s.csv' % {'caller':self.measured_fn_name, 'datestamp':datestamp})
        summary_path = os.path.join(output_path, '%(datestamp)s_summary_%(caller)s.csv' % {'caller':self.measured_fn_name, 'datestamp':datestamp})

        # Ensure that the output path exists (JR)
        if not os.path.exists(output_path):
            os.mkdir(output_path)

        with open(timelog_path, 'w', 1, encoding='utf-8') as log:
            log.write('timestamp,action\n')
            for event in self.timelog:
                log.write('%(ts)s,%(ev)s\n' % ({'ts':str(event[0]), 'ev':str(event[1])}))

        with open(counts_path, 'w', 1, encoding='utf-8') as log:
            log.write('event,n\n')
            for event, count in self.counter.items():
                log.write('%(event)s,%(count)s\n' % ({'event':str(event), 'count':str(count)}))

        with open(summary_path, 'w', 1, encoding='utf-8') as log:
            log.write('measure,value\n')
            for stat, val in summary.items():
                log.write('%(stat)s,%(val)s\n' % ({'stat':str(stat), 'val':str(val)}))

