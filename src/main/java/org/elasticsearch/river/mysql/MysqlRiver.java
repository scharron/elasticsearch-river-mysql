/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.mysql;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Closeables;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.*;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.deleteRequest;
import static org.elasticsearch.client.Requests.indexRequest;

/**
 *
 */
public class MysqlRiver extends AbstractRiverComponent implements River {

	private final Client client;

	private final String riverIndexName;

	/* Mysql stream reader configuration */
	private final String mysqlStreamProtocol;
	private final String mysqlStreamHost;
	private final int mysqlStreamPort;
	private final boolean httpsNoVerify;

	/* Mysql server configuration (Passed to the mysql stream reader) */ 
	private final String mysqlHost;
	private final int mysqlPort;
	private final String mysqlDb;
	private final String mysqlUser;
	private final String mysqlPassword;

	/* Index settings */
	private final String indexName;
	private final String typeName;
	private final int bulkSize;
	private final TimeValue bulkTimeout;
	private final int throttleSize;

	private final ExecutableScript script;

	private volatile Thread slurperThread;
	private volatile Thread indexerThread;
	private volatile boolean closed;

	private final BlockingQueue<String> stream;

	@SuppressWarnings({"unchecked"})
	@Inject
	public MysqlRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName, Client client, ScriptService scriptService) {
		super(riverName, settings);
		this.riverIndexName = riverIndexName;
		logger.warn("River index name {}", this.riverIndexName);
		this.client = client;

		if (settings.settings().containsKey("streamer")) {
			Map<String, Object> streamerSettings = (Map<String, Object>) settings.settings().get("streamer");
			mysqlStreamHost = XContentMapValues.nodeStringValue(streamerSettings.get("host"), "127.0.0.1");
			mysqlStreamPort = XContentMapValues.nodeIntegerValue(streamerSettings.get("port"), 8080);
			mysqlStreamProtocol = XContentMapValues.nodeStringValue(streamerSettings.get("protocol"), "http");
			httpsNoVerify = XContentMapValues.nodeBooleanValue(streamerSettings.get("verify_https"), true);

		} else {
			mysqlStreamHost = "127.0.0.1";
			mysqlStreamProtocol = "http";
			mysqlStreamPort = 8080;
			httpsNoVerify = false;
		}

		if (settings.settings().containsKey("mysql")) {
			Map<String, Object> mysqlSettings = (Map<String, Object>) settings.settings().get("mysql");
			mysqlHost = XContentMapValues.nodeStringValue(mysqlSettings.get("host"), "localhost");
			mysqlPort = XContentMapValues.nodeIntegerValue(mysqlSettings.get("port"), 3306);
			mysqlUser = XContentMapValues.nodeStringValue(mysqlSettings.get("user"), "root");
			mysqlPassword = XContentMapValues.nodeStringValue(mysqlSettings.get("password"), "");
			mysqlDb = XContentMapValues.nodeStringValue(mysqlSettings.get("db"), riverName.name());

			if (mysqlSettings.containsKey("script")) {
				String scriptType = "js";
				if(mysqlSettings.containsKey("scriptType")) {
					scriptType = mysqlSettings.get("scriptType").toString();
				}

				script = scriptService.executable(scriptType, mysqlSettings.get("script").toString(), Maps.newHashMap());
			} else {
				script = null;
			}
		} else {
			mysqlHost = "localhost";
			mysqlPort = 3306;
			mysqlUser = "root";
			mysqlPassword = "";
			mysqlDb = "db";
			script = null;
		}

		if (settings.settings().containsKey("index")) {
			Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
			indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), mysqlDb);
			typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), mysqlDb);
			bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
			if (indexSettings.containsKey("bulk_timeout")) {
				bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "10ms"), TimeValue.timeValueMillis(10));
			} else {
				bulkTimeout = TimeValue.timeValueMillis(10);
			}
			throttleSize = XContentMapValues.nodeIntegerValue(indexSettings.get("throttle_size"), bulkSize * 5);
		} else {
			indexName = mysqlDb;
			typeName = mysqlDb;
			bulkSize = 100;
			bulkTimeout = TimeValue.timeValueMillis(10);
			throttleSize = bulkSize * 5;
		}
		if (throttleSize == -1) {
			stream = new LinkedTransferQueue<String>();
		} else {
			stream = new ArrayBlockingQueue<String>(throttleSize);
		}
	}

	@Override
	public void start() {
		logger.info("starting mysql stream: {}://{}:{} (mysql://{}:{}@{}:{}/{}) indexing to [{}]/[{}]", mysqlStreamProtocol, mysqlStreamHost, mysqlStreamPort, mysqlUser, mysqlPassword, mysqlHost, mysqlPort, mysqlDb, indexName, typeName);
		try {
			client.admin().indices().prepareCreate(indexName).execute().actionGet();
		} catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
				// that's fine
			} else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
				// ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
				// TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the block is removed...
			} else {
				logger.warn("failed to create index [{}], disabling river...", e, indexName);
				return;
			}
		}

		slurperThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "mysql_river_slurper").newThread(new Slurper());
		indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "mysql_river_indexer").newThread(new Indexer());
		indexerThread.start();
		slurperThread.start();
	}

	@Override
	public void close() {
		if (closed) {
			return;
		}
		logger.info("closing mysql stream river");
		slurperThread.interrupt();
		indexerThread.interrupt();
		closed = true;
	}

	@SuppressWarnings({"unchecked"})
	private void processLine(String s, BulkRequestBuilder bulk) {
		Map<String, Object> ctx;
		try {
			ctx = XContentFactory.xContent(XContentType.JSON).createParser(s).mapAndClose();
		} catch (IOException e) {
			logger.warn("failed to parse {}", e, s);
			return;
		}

		if (script != null) {
			script.setNextVar("ctx", ctx);
			try {
				script.run();
				// we need to unwrap the ctx...
				ctx = (Map<String, Object>) script.unwrap(ctx);
			} catch (Exception e) {
				logger.warn("failed to script process {}, ignoring", e, ctx);
			}
		}

		Object action = ctx.get("action");
		String index = ctx.get("index") != null && ctx.get("index") instanceof String ? (String)ctx.get("index") : indexName;
		String type = ctx.get("type") != null && ctx.get("type") instanceof String ? (String)ctx.get("type") : typeName;
		String route = ctx.get("route") != null && ctx.get("route") instanceof String ? (String)ctx.get("route") : null;
		String parent = ctx.get("parent") != null && ctx.get("parent") instanceof String ? (String)ctx.get("parent") : null;


		String id = ctx.get("id") != null ? ctx.get("id").toString() : null;
		if (id == null) {
			logger.warn("Failed to process the request (null id provided), source {}", s);
			return;
		}

		if (action.equals("delete")) {
			logger.info("DELETING {}", id);
			bulk.add(deleteRequest(index).type(type).id(id).routing(route).parent(parent));
		} else if (action.equals("update") || action.equals("insert")) {
			logger.info("INSERTING / UPDATING {}", id);
			if (ctx.get("doc") == null || !(ctx.get("doc") instanceof Map<?, ?>))
				logger.warn("No document supplied");
			else {
				Map<String, Object> doc = (Map<String, Object>) ctx.get("doc");

				if (logger.isTraceEnabled()) {
					logger.trace("processing [index ]: [{}]/[{}]/[{}], source {}", index, type, id, doc);
				}

				bulk.add(indexRequest(index).type(type).id(id).source(doc).routing(route).parent(parent));
			}
		} else {
			logger.warn("ignoring unknown change {}", s);
		}
		return;
	}

	private class Indexer implements Runnable {
		@Override
		public void run() {
			while (true) {
				if (closed) {
					return;
				}
				String s;
				try {
					s = stream.take();
				} catch (InterruptedException e) {
					logger.warn("Interrupted ? [1]", e);
					if (closed) {
						return;
					}
					continue;
				}
				BulkRequestBuilder bulk = client.prepareBulk();
				logger.warn("Got line {}", s);
				processLine(s, bulk);
				
				// spin a bit to see if we can get some more changes
				try {
					while ((s = stream.poll(bulkTimeout.millis(), TimeUnit.MILLISECONDS)) != null) {
						processLine(s, bulk);
						
						if (bulk.numberOfActions() >= bulkSize) {
							break;
						}
					}
				} catch (InterruptedException e) {
					logger.warn("Interrupted ? [2]", e);
					if (closed) {
						return;
					}
				}

				try {
					BulkResponse response = bulk.execute().actionGet();
					if (response.hasFailures()) {
						// TODO write to exception queue?
						logger.info("failed to execute" + response.buildFailureMessage());
					}
				} catch (Exception e) {
					logger.info("failed to execute bulk", e);
				}
			}
		}
	}


	private class Slurper implements Runnable {
	@Override
		public void run() {

			while (true) {
				if (closed) {
					return;
				}

				HttpURLConnection connection = null;
				InputStream is = null;
				try {
					logger.info("connected");

					URL url = new URL(mysqlStreamProtocol, mysqlStreamHost, mysqlStreamPort, "/");
					connection = (HttpURLConnection) url.openConnection();
					connection.setDoInput(true);
					connection.setUseCaches(false);

					if (httpsNoVerify) {
						((HttpsURLConnection) connection).setHostnameVerifier(
						                                                      new HostnameVerifier() {
						                                                    	  public boolean verify(String string, SSLSession ssls) {
						                                                    		  return true;
						                                                    	  }
						                                                      }
								);
					}

					logger.info("streaming ok");

					is = connection.getInputStream();

					final BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));

					String line;
					while ((line = reader.readLine()) != null) {
						logger.info("reading");
						if (closed) {
							return;
						}
						if (line.length() == 0) {
							logger.trace("[mysql] heartbeat");
							continue;
						}
						logger.warn("[mysql] {}", line);

						if (logger.isTraceEnabled()) {
							logger.trace("[mysql] {}", line);
						}
						// we put here, so we block if there is no space to add
						stream.put(line);
					}
				} catch (Exception e) {
					Closeables.closeQuietly(is);
					if (connection != null) {
						try {
							connection.disconnect();
						} catch (Exception e1) {
							// ignore
						} finally {
							connection = null;
						}
					}
					if (closed) {
						return;
					}
					logger.warn("failed to read, throttling....", e);
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e1) {
						if (closed) {
							return;
						}
					}
				} finally {
					Closeables.closeQuietly(is);
					if (connection != null) {
						try {
							connection.disconnect();
						} catch (Exception e1) {
							// ignore
						} finally {
							connection = null;
						}
					}
				}
			}
		}
	}
}
