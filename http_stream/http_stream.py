#!/usr/bin/env python
#
# Update a redis server cache when an evenement is trigger
# in MySQL replication log
#

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import *

mysql_settings = {'host': '127.0.0.1', 'port': 3306, 'user': 'root', 'passwd': ''}

import json
import cherrypy
class Streamer(object):
    def __init__(self):
        self.stream = BinLogStreamReader(connection_settings = mysql_settings,
                                         only_events = [DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent], blocking = True, resume_stream = True)


    def index(self):
        cherrypy.response.headers['Content-Type'] = 'text/plain'
        def content():
            for binlogevent in self.stream:
                for row in binlogevent.rows:
                    if isinstance(binlogevent, DeleteRowsEvent):
                        yield json.dumps({
                          "action": "delete",
                          "id": row["values"]["id"]}) + "\n"
                    elif isinstance(binlogevent, UpdateRowsEvent):
                        yield json.dumps({
                          "action": "update",
                          "id": row["after_values"]["id"],
                          "doc": row["after_values"]}) + "\n"
                    elif isinstance(binlogevent, WriteRowsEvent):
                        yield json.dumps({
                          "action": "insert",
                          "id": row["values"]["id"],
                          "doc": row["values"]}) + "\n"
        return content()

    index.exposed = True
    index._cp_config = {"response.stream": True}

cherrypy.quickstart(Streamer())
