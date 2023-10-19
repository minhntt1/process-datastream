package com.example.datastream.service;

import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Queue;

import com.example.datastream.model.EdgeKey;
import com.example.datastream.model.EdgeKeyTimestamp;
import com.example.datastream.model.EdgeValue;
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
		KeyValue statusKey = this.getKey("http.status_code", data);

		if (statusKey != null)
			return statusKey.getValue().getIntValue() >= 400;

		return data.getStatus().getCode() == StatusCode.STATUS_CODE_ERROR;
	}

	public boolean chkEdgeOk(EdgeValue edgeValue) {
		return edgeValue.getClient() != null && edgeValue.getServer() != null;
	}

	public long convertToBucket(long tsNs) {
		return tsNs / this.bucketNs * this.bucketNs;
	}

	public void startBatch() {
		this.cassandra.startBatch();
	}
	
	public void genBatchQuery(Long bucket, Long ts, String client, String server, String connType, Long serverDurUs, Boolean isErr) {
		this.cassandra.genBatchQuery(bucket, ts, client, server, connType, serverDurUs, isErr);
	}
	
	public void sendBatch() {
		this.cassandra.sendBatch();
	}
	
	public void insertData(Long bucket, Long ts, String client, String server, String connType, Long serverDurUs,
			Boolean isErr) {
		this.cassandra.insert(bucket, ts, client, server, connType, serverDurUs, isErr);
	}
	
	public void insertPrepared(Long bucket, Long ts, String client, String server, String connType, Long serverDurUs, Boolean isErr) {
		this.cassandra.insertPrepared(bucket, ts, client, server, connType, serverDurUs, isErr);
	}

	public KeyValue getKey(String name, Span span) {
		return span.getAttributesList().stream().filter(x -> x.getKey().equals(name)).findFirst().orElse(null);
	}

	public KeyValue getKeyResource(String name, ResourceSpans resourceSpans) {
		return resourceSpans.getResource().getAttributesList().stream().filter(x -> x.getKey().equals(name)).findFirst()
				.orElse(null);
	}
	
	public void insertDataClientProducer(Queue<EdgeKeyTimestamp> order, HashMap<EdgeKey, EdgeValue> incompleteEdges, ByteString traceId, ByteString spanId, Long startTimeNs, Long bucketTime, Long durationUs, String currService, Boolean spanErr, String connType) {
		EdgeKey edgeKey = new EdgeKey(traceId, spanId);
		if (incompleteEdges.containsKey(edgeKey)) {
			EdgeValue edgeValue = incompleteEdges.get(edgeKey);
			edgeValue.setBucket(bucketTime);
			edgeValue.setTs(startTimeNs);
			edgeValue.setClient(currService);
			edgeValue.setIsErr(edgeValue.getIsErr() || spanErr);
			if (this.chkEdgeOk(edgeValue)) {
				// insert
				this.insertPrepared(edgeValue.getBucket(), edgeValue.getTs(), edgeValue.getClient(), edgeValue.getServer(), edgeValue.getConn(), edgeValue.getServerDur(), edgeValue.getIsErr());
//				this.genBatchQuery(edgeValue.getBucket(), edgeValue.getTs(), edgeValue.getClient(), edgeValue.getServer(), edgeValue.getConn(), edgeValue.getServerDur(), edgeValue.getIsErr());
				incompleteEdges.remove(edgeKey);
			}
		} else {
			order.add(new EdgeKeyTimestamp(edgeKey, startTimeNs));
			incompleteEdges.put(edgeKey, new EdgeValue(bucketTime, startTimeNs, currService, null,
					connType, spanErr, null));
		}
	}
	
	public void insertDataServerConsumer(Queue<EdgeKeyTimestamp> order, HashMap<EdgeKey, EdgeValue> incompleteEdges, ByteString traceId, ByteString parSpanId, Long startTimeNs, Long bucketTime, Long durationUs, String currService, Boolean spanErr, String connType) {
		EdgeKey edgeKey = new EdgeKey(traceId, parSpanId);
		if (incompleteEdges.containsKey(edgeKey)) {
			EdgeValue edgeValue = incompleteEdges.get(edgeKey);
			edgeValue.setServer(currService);
			edgeValue.setServerDur(durationUs);
			edgeValue.setIsErr(edgeValue.getIsErr() || spanErr);
			if (this.chkEdgeOk(edgeValue)) {
				// insert
				this.insertPrepared(edgeValue.getBucket(), edgeValue.getTs(), edgeValue.getClient(), edgeValue.getServer(), edgeValue.getConn(), edgeValue.getServerDur(), edgeValue.getIsErr());
//				this.genBatchQuery(edgeValue.getBucket(), edgeValue.getTs(), edgeValue.getClient(), edgeValue.getServer(), edgeValue.getConn(), edgeValue.getServerDur(), edgeValue.getIsErr());
				incompleteEdges.remove(edgeKey);
			}
		} else {
			order.add(new EdgeKeyTimestamp(edgeKey, startTimeNs));
			incompleteEdges.put(edgeKey,
					new EdgeValue(null, null, null, currService, connType, spanErr, durationUs));
		}
	}

	public void close() {
		this.cassandra.close();
	}

	public void handleTraceData(
			HashMap<EdgeKey, EdgeValue> incompleteEdges, 
			Queue<EdgeKeyTimestamp> order,
			TracesData data) {
		List<ResourceSpans> list = data.getResourceSpansList();
		for (ResourceSpans resourceSpans : list) {
			KeyValue keyServiceName = this.getKeyResource("service.name", resourceSpans);

			if (keyServiceName == null)
				return;

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

					System.out.printf("trace=%s, span=%s, par=%s\n", HexFormat.of().formatHex(traceId.toByteArray()),
							HexFormat.of().formatHex(spanId.toByteArray()),
							parSpanId != null ? HexFormat.of().formatHex(parSpanId.toByteArray()) : null);

					if (kind == SpanKind.SPAN_KIND_CLIENT) {
						KeyValue dbKey = this.getKey("db.name", span);

						if (dbKey != null) {
							String dbSystem = this.getKey("db.system", span).getValue().getStringValue();
							connType = "database";
							String client = currService;
							String server = new StringBuilder().append("db").append('_').append(dbSystem).append('_').append(dbKey.getValue().getStringValue()).toString();
							Long serverDur = durationUs;
							Boolean err = spanErr;
							// insert db
							this.insertPrepared(bucketTime, startTimeNs, client, server, connType, serverDur, err);
//							this.genBatchQuery(bucketTime, startTimeNs, client, server, connType, serverDur, err);
							continue;
						}
						else {
							connType = "endpoint";
						}
						
						this.insertDataClientProducer(order, incompleteEdges, traceId, spanId, startTimeNs, bucketTime, durationUs, currService, spanErr, connType);
					} else if (kind == SpanKind.SPAN_KIND_PRODUCER) {
						connType = "async";
						
						this.insertDataClientProducer(order, incompleteEdges, traceId, spanId, startTimeNs, bucketTime, durationUs, currService, spanErr, connType);
					} else if (kind == SpanKind.SPAN_KIND_SERVER) {
						KeyValue targetKey = this.getKey("http.target", span);

						if (targetKey != null) {
							String target = targetKey.getValue().getStringValue();
							String method = this.getKey("http.method", span).getValue().getStringValue();
							connType = "endpoint";
							currService = new StringBuilder().append(serviceName).append('_').append(method).append('_').append(target).toString();
							String client = serviceName;
							String server = currService;
							Long serverDur = durationUs;
							Boolean err = spanErr;
							// insert
							this.insertPrepared(bucketTime, startTimeNs, client, server, connType, serverDur, err);
//							this.genBatchQuery(bucketTime, startTimeNs, client, server, connType, serverDur, err);
						}

						if (parSpanId == ByteString.EMPTY) {
							this.insertPrepared(this.convertToBucket(startTimeNs - 1), startTimeNs - 1, "random-client", currService, connType, durationUs, spanErr);
//							this.genBatchQuery(this.convertToBucket(startTimeNs - 1), startTimeNs - 1, "random-client", currService, connType, durationUs, spanErr);
							continue;
						}

						this.insertDataServerConsumer(order, incompleteEdges, traceId, parSpanId, startTimeNs, bucketTime, durationUs, currService, spanErr, connType);
					} else if (kind == SpanKind.SPAN_KIND_CONSUMER) {
						KeyValue systemKey = this.getKey("messaging.system", span);
						
						KeyValue lengthKey = this.getKey("messaging.message.payload_size_bytes", span);
						
						//get rid of processing
						if(lengthKey != null && lengthKey.getValue().getIntValue() == 0)
							continue;
						
						if (systemKey != null) {
							String system = systemKey.getValue().getStringValue();
							String dest = this.getKey("messaging.destination.name", span).getValue().getStringValue();
							connType = "async";
							currService = new StringBuilder().append("queue").append('_').append(system).append('_').append(dest).toString();
							String client = currService;
							String server = serviceName;
							Long serverDur = durationUs;
							Boolean err = spanErr;
							// insert
							this.insertPrepared(this.convertToBucket(startTimeNs), startTimeNs, client, server, connType, serverDur, err);
//							this.genBatchQuery(this.convertToBucket(startTimeNs), startTimeNs, client, server, connType, serverDur, err);
						}

						if (parSpanId == ByteString.EMPTY) {
							this.insertPrepared(this.convertToBucket(startTimeNs - 1), startTimeNs - 1, "random-producer", currService, connType, durationUs, spanErr);
//							this.genBatchQuery(this.convertToBucket(startTimeNs - 1), startTimeNs - 1, "random-producer", currService, connType, durationUs, spanErr);
							continue;
						}

						this.insertDataServerConsumer(order, incompleteEdges, traceId, parSpanId, startTimeNs, bucketTime, durationUs, currService, spanErr, connType);
					}

					//remove edges after 10secs from curr span
					if (!order.isEmpty() && span.getStartTimeUnixNano() > order.peek().getStartTs() + 1e10) {
						EdgeKey edgeKey = order.peek().getEdgeKey();
						if (incompleteEdges.containsKey(edgeKey))
							incompleteEdges.remove(edgeKey);
						order.remove();
					}
				}
			}
		}
	}
}
