Mysql River Plugin for ElasticSearch
====================================

The Mysql River plugin allows to hook into Mysql replication feed using the excellent 
[python-mysql-replication](https://github.com/noplay/python-mysql-replication) and automatically index it into elasticsearch.

This plugin is based on the [elasticsearch-river-couchdb](https://github.com/elasticsearch/elasticsearch-river-couchdb.git) plugin.



Installation
============

This river consists of two parts : 
 - one is the elasticsearch plugin
 - the other one is a little webserver to serve an http stream of the mysql replication stream.


The river
---------

To install this plugin, first build it using `mvn clean package`.
Then, use the plugin binary distributed with elasticsearch `/path/to/elasticsearch/bin/plugin -url file:./target/releases/elasticsearch-river-mysql-*-SNAPSHOT.zip -install mysql-river`

Mysql replication stream
------------------------

You first need to install dependencies : 
 - `easy_install pymysql`
 - `easy_install cherrypy`
 - `git clone git://github.com/noplay/python-mysql-replication.git ; cd python-mysql-replication ; python setup.py install`


Running the river
=================

 - `python http_stream/http_stream.py`
 - `/path/to/elasticsearch/bin/elasticsearch`


Plugin activation and configuration
===================================

  curl -XPUT 'localhost:9200/_river/mydb/_meta' -d '{
      "type" : "mysql",
      "streamer" : {
          "host" : "localhost",
          "port" : 8080,
      },
      "mysql" : {
        # Future usage to configure mysql access from the river.  _
      }
  }'



Editing the plugin
==================

To edit the plugin using eclipse, run `mvn eclipse:eclipse`, then you can import the current directory as an existing project in eclipse.


Bulking
======

Bulking is automatically done in order to speed up the indexing process. If within the specified **bulk_timeout** more changes are detected, changes will be bulked up to **bulk_size** before they are indexed.

License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2012 Shay Banon and ElasticSearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
