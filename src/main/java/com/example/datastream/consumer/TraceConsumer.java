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

import com.example.datastream.model.EdgeClient;
import com.example.datastream.model.EdgeKey;
import com.example.datastream.model.EdgeKeyTimestamp;
import com.example.datastream.model.EdgeServer;
import com.example.datastream.service.TimeDataService;

import io.opentelemetry.proto.trace.v1.TracesData;

public class TraceConsumer {
	private final KafkaConsumer<String, byte[]> consumer;
	private final TimeDataService service;
	private volatile boolean close = false;

	public TraceConsumer(TimeDataService service, String brokers, String user, String pass, String topic, String group) {
		this.service = service;
		String authStr = new StringBuilder()
				.append("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"").append(user)
				.append("\" password=\"").append(pass).append("\";").toString();
		Properties properties = new Properties();
		properties.put("bootstrap.servers", brokers);
		properties.put("group.id", group);
		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "2000");
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
	
	public void stop() {
		this.close = true;
	}

	public void close() {
		this.service.close();
		this.consumer.close();
	}

	public void consume() {
		HashMap<EdgeKey, LinkedList<EdgeServer>> incompleteEdges = new HashMap<>(); 
		HashMap<EdgeKey, EdgeClient> keyClient = new HashMap<>();
		Queue<EdgeKeyTimestamp> order = new LinkedList<>();
		
		try {
			while (!close) {
				ConsumerRecords<String, byte[]> records = this.consumer.poll(Duration.ofMillis(1000));
				
				for (ConsumerRecord<String, byte[]> record : records) {
					this.service.handleTraceData(incompleteEdges, keyClient, order, TracesData.parseFrom(record.value()));
				}
				
				//remove edges after 15mins from curr timestamp
				long curr = System.currentTimeMillis();
				while (!order.isEmpty() && curr > order.peek().getStartTs() + 9e5) {
					EdgeKey edgeKey = order.peek().getEdgeKey();
					incompleteEdges.get(edgeKey).clear();
					incompleteEdges.remove(edgeKey);
					keyClient.remove(edgeKey);
					order.remove();
				}
			}
		}
		catch (Exception exception) {
			exception.printStackTrace();
		}
		finally {
			this.close();
		}
	}
}
