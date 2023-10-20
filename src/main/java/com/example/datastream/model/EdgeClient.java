package com.example.datastream.model;

public class EdgeClient {
	private String client;
	private Long bucket;
	private Long ts;
	private String conn;
	public EdgeClient(String client, Long bucket, Long ts, String conn) {
		super();
		this.client = client;
		this.bucket = bucket;
		this.ts = ts;
		this.conn = conn;
	}
	public String getClient() {
		return client;
	}
	public void setClient(String client) {
		this.client = client;
	}
	public Long getBucket() {
		return bucket;
	}
	public void setBucket(Long bucket) {
		this.bucket = bucket;
	}
	public Long getTs() {
		return ts;
	}
	public void setTs(Long ts) {
		this.ts = ts;
	}
	public String getConn() {
		return conn;
	}
	public void setConn(String conn) {
		this.conn = conn;
	}
}
