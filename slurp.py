#!/usr/bin/python3 -u
#-*- codingL utf-8 -*-

'''
Simple code to read Wikipedia recent change events from the published stream,
filter them down to selected sources and omitted keys, and then output the
remaining info as a JSONL record to stdout.
'''

import re
import json
from sseclient import SSEClient as EventSource

### Set url to the source stream address to slurp from 
url = 'https://stream.wikimedia.org/v2/stream/recentchange'

### Any keys matching the regular expressions listed in omit_keys are dropped
omit_keys = [ 'comment' , 'log_action_comment', 'log_params.*', 'user\\.*',
              'meta', 'parsedcomment' ]

### Any records associated with a wiki not listed in incl_wikis are dropped
incl_wikis = [ 'enwik[a-z]*', 'simplewik[a-z]*' , 'wikidatawiki',
               'commonswiki' ]

### process_line drops any pairs whose key matches a regex in omit_keys
def process_line(line):
    pattern = re.compile(r'\b(?:%s)\b' % '|'.join(omit_keys))
    for k in list(line.keys()):
        if pattern.match(k):
            line.pop(k)
    print(json.dumps(line))

### filter_wiki processes records associated with wikis listed in incl_wikis
def filter_wiki(line):
    pattern = re.compile(r'\b(?:%s)\b' % '|'.join(incl_wikis))
    if not pattern.match(line['wiki']):
        return
    process_line(line)

### Main loop that reads from the change stream and processes messages 
for event in EventSource(url):
    if event.event == 'message':
        try:
            line = json.loads(event.data)
        except ValueError:
            pass
        else:
            filter_wiki(line)
