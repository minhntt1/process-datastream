package com.example.datastream.consumer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.example.datastream.model.EdgeKey;
import com.example.datastream.model.EdgeKeyTimestamp;
import com.example.datastream.model.EdgeValue;
import com.example.datastream.service.TimeDataService;
import com.google.protobuf.InvalidProtocolBufferException;

import io.opentelemetry.proto.trace.v1.TracesData;

public class TraceConsumer {
	private final KafkaConsumer<String, byte[]> consumer;
	private final TimeDataService service;

	public TraceConsumer(TimeDataService service, String brokers, String user, String pass, String topic, String group) {
		this.service = service;
		String authStr = new StringBuilder()
				.append("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"").append(user)
				.append("\" password=\"").append(pass).append("\";").toString();
		Properties properties = new Properties();
		properties.put("bootstrap.servers", brokers);
		properties.put("group.id", group);
		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "100");
		properties.put("auto.offset.reset", "latest");
		properties.put("session.timeout.ms", "15000");
		properties.put("key.deserializer", StringDeserializer.class.getName());
		properties.put("value.deserializer", ByteArrayDeserializer.class.getName());
		properties.put("security.protocol", "SASL_SSL");
		properties.put("sasl.mechanism", "SCRAM-SHA-512");
		properties.put("sasl.jaas.config", authStr);
		this.consumer = new KafkaConsumer<>(properties);
		this.consumer.subscribe(Arrays.asList(topic));
	}

	public void close() {
		this.service.close();
		this.consumer.close();
	}

	public void consume() throws InvalidProtocolBufferException {
		HashMap<EdgeKey, EdgeValue> incompleteEdges = new HashMap<EdgeKey, EdgeValue>(); 
		Queue<EdgeKeyTimestamp> order = new LinkedList<>();
		
		while (true) {
			ConsumerRecords<String, byte[]> records = this.consumer.poll(Duration.ofMillis(100));
			
			for (ConsumerRecord<String, byte[]> record : records) {
//				this.service.startBatch();
				this.service.handleTraceData(incompleteEdges, order, TracesData.parseFrom(record.value()));
//				this.service.sendBatch();
			}
			
			long currentTs = System.currentTimeMillis()*1000000;
			//remove after 30 secs
			while (!order.isEmpty() && currentTs > order.peek().getStartTs() + 3e10) {
				EdgeKey edgeKey = order.peek().getEdgeKey();
				if (incompleteEdges.containsKey(edgeKey))
					incompleteEdges.remove(edgeKey);
				order.remove();
			}
		}
	}
}
