package com.example.datastream.model;
public class EdgeValue {
	private Long bucket;
	private Long ts;
	private String client;
	private String server;
	private String conn;
	private Boolean isErr;
	private Long serverDur;
	public EdgeValue(Long bucket, Long ts, String client, String server, String conn, Boolean isErr, Long serverDur) {
		super();
		this.bucket = bucket;
		this.ts = ts;
		this.client = client;
		this.server = server;
		this.conn = conn;
		this.isErr = isErr;
		this.serverDur = serverDur;
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
	public String getClient() {
		return client;
	}
	public void setClient(String client) {
		this.client = client;
	}
	public String getServer() {
		return server;
	}
	public void setServer(String server) {
		this.server = server;
	}
	public String getConn() {
		return conn;
	}
	public void setConn(String conn) {
		this.conn = conn;
	}
	public Boolean getIsErr() {
		return isErr;
	}
	public void setIsErr(Boolean isErr) {
		this.isErr = isErr;
	}
	public Long getServerDur() {
		return serverDur;
	}
	public void setServerDur(Long serverDur) {
		this.serverDur = serverDur;
	}
}
