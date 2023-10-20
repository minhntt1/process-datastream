package com.example.datastream.service;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.example.datastream.model.EdgeClient;
import com.example.datastream.model.EdgeKey;
import com.example.datastream.model.EdgeKeyTimestamp;
import com.example.datastream.model.EdgeServer;
import com.example.datastream.repo.TimeDataRepo;
import com.google.protobuf.ByteString;

import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Span.SpanKind;
import io.opentelemetry.proto.trace.v1.Status.StatusCode;
import io.opentelemetry.proto.trace.v1.TracesData;

public class TimeDataService {
	private final long bucketNs;
	private final TimeDataRepo cassandra;

	public TimeDataService(long bucketNs, TimeDataRepo cassandra) {
		super();
		this.bucketNs = bucketNs;
		this.cassandra = cassandra;
	}

	public boolean chkSpanErr(Span data) {
		KeyValue statusKey = this.getKey("http.status_code", data.getAttributesList());

		if (statusKey != null)
			return statusKey.getValue().getIntValue() >= 400;

		return data.getStatus().getCode() == StatusCode.STATUS_CODE_ERROR;
	}

	public long convertToBucket(long tsNs) {
		return tsNs / this.bucketNs * this.bucketNs;
	}

	public KeyValue getKey(String name, List<KeyValue> spanAttri) {
		for(KeyValue keyValue:spanAttri)
			if(keyValue.getKey().equals(name))
				return keyValue;
		return null;
	}
	
	public void insertDataClientProducer(Queue<EdgeKeyTimestamp> order, HashMap<EdgeKey, LinkedList<EdgeServer>> incompleteEdges, HashMap<EdgeKey, EdgeClient> keyClient,  ByteString traceId, ByteString spanId, Long startTimeNs, Long bucketTime, String currService, String connType) {
		EdgeKey edgeKey = new EdgeKey(traceId, spanId);
		
		if(!keyClient.containsKey(edgeKey))
			keyClient.put(edgeKey, new EdgeClient(currService, bucketTime, startTimeNs, connType));
		
		LinkedList<EdgeServer> servers = incompleteEdges.get(edgeKey);
		if (servers != null) {
			EdgeClient edgeClient = keyClient.get(edgeKey);
			
			while(!servers.isEmpty()) {
				EdgeServer edgeServer = servers.peek();
				this.cassandra.insertPrepared(edgeClient.getBucket(), edgeClient.getTs(), edgeServer.getServerTs(), edgeClient.getClient(), edgeServer.getServer(), edgeClient.getConn(), edgeServer.getServerDur(), edgeServer.getIsErr());
				servers.remove();
			}
		} else {
			order.add(new EdgeKeyTimestamp(edgeKey, startTimeNs));
			incompleteEdges.put(edgeKey, new LinkedList<>());
		}
	}
	
	public void insertDataServerConsumer(Queue<EdgeKeyTimestamp> order, HashMap<EdgeKey, LinkedList<EdgeServer>> incompleteEdges, HashMap<EdgeKey, EdgeClient> keyClient, ByteString traceId, ByteString parSpanId, Long startTimeNs, Long bucketTime, Long durationUs, String currService, Boolean spanErr, String connType) {
		EdgeKey edgeKey = new EdgeKey(traceId, parSpanId);
		
		if(!keyClient.containsKey(edgeKey)) {
			if (!incompleteEdges.containsKey(edgeKey)) {
				order.add(new EdgeKeyTimestamp(edgeKey, startTimeNs));
				incompleteEdges.put(edgeKey, new LinkedList<>());
			}
			
			incompleteEdges.get(edgeKey).add(new EdgeServer(startTimeNs, currService, spanErr, durationUs));
		}
		else {
			EdgeClient edgeClient = keyClient.get(edgeKey);
			this.cassandra.insertPrepared(edgeClient.getBucket(), edgeClient.getTs(), startTimeNs, edgeClient.getClient(), currService, edgeClient.getConn(), durationUs, spanErr);
		}
	}

	public void close() {
		this.cassandra.close();
	}

	public void handleTraceData(
			HashMap<EdgeKey, LinkedList<EdgeServer>> incompleteEdges, 
			HashMap<EdgeKey, EdgeClient> keyClient, 
			Queue<EdgeKeyTimestamp> order,
			TracesData data) {
		List<ResourceSpans> list = data.getResourceSpansList();
		for (ResourceSpans resourceSpans : list) {
			KeyValue keyServiceName = this.getKey("service.name", resourceSpans.getResource().getAttributesList());

			if (keyServiceName == null)
				continue;

			String serviceName = keyServiceName.getValue().getStringValue();
			List<ScopeSpans> scopeSpans = resourceSpans.getScopeSpansList();

			for (ScopeSpans scopeSpan : scopeSpans) {
				List<Span> spans = scopeSpan.getSpansList();

				for (Span span : spans) {
					ByteString traceId = span.getTraceId();
					ByteString spanId = span.getSpanId();
					Long durationUs = (span.getEndTimeUnixNano() - span.getStartTimeUnixNano()) / 1000;
					Long startTimeNs = span.getStartTimeUnixNano();
					Long bucketTime = this.convertToBucket(startTimeNs);
					Boolean spanErr = this.chkSpanErr(span);
					SpanKind kind = span.getKind();
					String connType = "";
					String currService = serviceName;
					ByteString parSpanId = span.getParentSpanId();
					
					if (kind == SpanKind.SPAN_KIND_CLIENT) {
						KeyValue dbKey = this.getKey("db.name", span.getAttributesList());

						if (dbKey != null) {
							String dbSystem = this.getKey("db.system", span.getAttributesList()).getValue().getStringValue();
							connType = "database";
							String client = currService;
							String server = new StringBuilder().append("db").append('_').append(dbSystem).append('_').append(dbKey.getValue().getStringValue()).toString();
							Long serverDur = durationUs;
							Boolean err = spanErr;
							// insert db
							this.cassandra.insertPrepared(bucketTime, startTimeNs, startTimeNs, client, server, connType, serverDur, err);
							continue;
						}
						else {
							connType = "endpoint";
						}
						
						this.insertDataClientProducer(order, incompleteEdges, keyClient, traceId, spanId, startTimeNs, bucketTime, currService, connType);
					} else if (kind == SpanKind.SPAN_KIND_PRODUCER) {
						connType = "async";
						
						this.insertDataClientProducer(order, incompleteEdges, keyClient, traceId, spanId, startTimeNs, bucketTime, currService, connType);
					} else if (kind == SpanKind.SPAN_KIND_SERVER) {
						KeyValue targetKey = this.getKey("http.target", span.getAttributesList());

						if (targetKey != null) {
							String target = targetKey.getValue().getStringValue();
							String method = this.getKey("http.method", span.getAttributesList()).getValue().getStringValue();
							connType = "endpoint";
							currService = new StringBuilder().append(serviceName).append('_').append(method).append('_').append(target).toString();
							String client = serviceName;
							String server = currService;
							Long serverDur = durationUs;
							Boolean err = spanErr;
							// insert
							this.cassandra.insertPrepared(bucketTime, startTimeNs, startTimeNs, client, server, connType, serverDur, err);
						}

						if (parSpanId == ByteString.EMPTY) {
							this.cassandra.insertPrepared(this.convertToBucket(startTimeNs - 1), startTimeNs - 1, startTimeNs, "random-client", currService, connType, durationUs, spanErr);
							continue;
						}

						this.insertDataServerConsumer(order, incompleteEdges, keyClient, traceId, parSpanId, startTimeNs, bucketTime, durationUs, currService, spanErr, connType);
					} else if (kind == SpanKind.SPAN_KIND_CONSUMER) {
						KeyValue systemKey = this.getKey("messaging.system", span.getAttributesList());
						
						KeyValue lengthKey = this.getKey("messaging.message.payload_size_bytes", span.getAttributesList());
						
						//get rid of processing
						if(lengthKey != null && lengthKey.getValue().getIntValue() == 0)
							continue;
						
						if (systemKey != null) {
							String system = systemKey.getValue().getStringValue();
							String dest = this.getKey("messaging.destination.name", span.getAttributesList()).getValue().getStringValue();
							connType = "async";
							currService = new StringBuilder().append("queue").append('_').append(system).append('_').append(dest).toString();
							String client = currService;
							String server = serviceName;
							Long serverDur = durationUs;
							Boolean err = spanErr;
							// insert
							this.cassandra.insertPrepared(bucketTime, startTimeNs, startTimeNs, client, server, connType, serverDur, err);
						}

						if (parSpanId == ByteString.EMPTY) {
							this.cassandra.insertPrepared(this.convertToBucket(startTimeNs - 1), startTimeNs - 1, startTimeNs, "random-producer", currService, connType, durationUs, spanErr);
							continue;
						}

						this.insertDataServerConsumer(order, incompleteEdges, keyClient, traceId, parSpanId, startTimeNs, bucketTime, durationUs, currService, spanErr, connType);
					}

					//remove edges after 10secs from curr span
					if (!order.isEmpty() && span.getStartTimeUnixNano() > order.peek().getStartTs() + 1e10) {
						EdgeKey edgeKey = order.peek().getEdgeKey();
						incompleteEdges.get(edgeKey).clear();
						incompleteEdges.remove(edgeKey);
						keyClient.remove(edgeKey);
						order.remove();
					}
				}
			}
		}
	}
}
