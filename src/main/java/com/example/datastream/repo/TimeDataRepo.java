package com.example.datastream.repo;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class TimeDataRepo {
	private final Session cassandraSession;
	private final StringBuilder batchQuery;
	private final PreparedStatement preparedInsertStatement;

	public TimeDataRepo(String host, Integer port, String user, String pass) {
		Builder builder = Cluster.builder();
		builder.addContactPoint(host);
		builder.withPort(port);
		builder.withCredentials(user, pass);
		builder.withSSL();
		this.cassandraSession = builder.build().connect();
		this.batchQuery = new StringBuilder();
		this.preparedInsertStatement = this.cassandraSession.prepare("insert into jaeger_v1_dc1.v4_time_data(bucket,ts,ts_server,client,server,conn_type,server_err,server_dur) values (?,?,?,?,?,?,?,?);");
	}

	public void startBatch() {
		this.batchQuery.setLength(0);
		this.batchQuery.append("begin unlogged batch ");
	}

	public void genBatchQuery(Long bucket, Long ts, Long tsServer, String client, String server, String connType, Long serverDurNs, Boolean isErr) {
		this.batchQuery.append("insert into jaeger_v1_dc1.v4_time_data(bucket,ts,ts_server,client,server,conn_type,server_err,server_dur) ");
		this.batchQuery.append("values (");
		this.batchQuery.append(bucket).append(",");
		this.batchQuery.append(ts).append(",");
		this.batchQuery.append(tsServer).append(",");
		this.batchQuery.append("'").append(client).append("'").append(",");
		this.batchQuery.append("'").append(server).append("'").append(",");
		this.batchQuery.append("'").append(connType).append("'").append(",");
		this.batchQuery.append(isErr).append(",");
		this.batchQuery.append(serverDurNs);
		this.batchQuery.append(");");
	}

	public void sendBatch() {
		this.batchQuery.append("apply batch;");
		this.cassandraSession.executeAsync(this.batchQuery.toString());
	}

	public void insert(Long bucket, Long ts, Long tsServer, String client, String server, String connType, Long serverDurNs, Boolean isErr) {
		StringBuilder builder = new StringBuilder();
		builder.append("insert into jaeger_v1_dc1.v4_time_data(bucket,ts,ts_server,client,server,conn_type,server_err,server_dur) ");
		builder.append("values (");
		builder.append(bucket).append(",");
		builder.append(ts).append(",");
		builder.append(tsServer).append(",");
		builder.append("'").append(client).append("'").append(",");
		builder.append("'").append(server).append("'").append(",");
		builder.append("'").append(connType).append("'").append(",");
		builder.append(isErr).append(",");
		builder.append(serverDurNs);
		builder.append(");");
		this.cassandraSession.executeAsync(builder.toString());
	}

	public void insertPrepared(Long bucket, Long ts, Long tsServer, String client, String server, String connType, Long serverDurNs, Boolean isErr) {
		this.cassandraSession.executeAsync(this.preparedInsertStatement.bind(bucket, ts, tsServer, client, server, connType, isErr, serverDurNs));
	}

	public void close() {
		this.cassandraSession.close();
	}
}
