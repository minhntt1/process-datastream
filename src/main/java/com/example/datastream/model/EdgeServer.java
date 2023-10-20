package com.example.datastream.model;

public class EdgeServer {
	private Long serverTs;
	private String server;
	private Boolean isErr;
	private Long serverDur;
	public EdgeServer(Long serverTs, String server, Boolean isErr, Long serverDur) {
		super();
		this.serverTs = serverTs;
		this.server = server;
		this.isErr = isErr;
		this.serverDur = serverDur;
	}
	public Long getServerTs() {
		return serverTs;
	}
	public void setServerTs(Long serverTs) {
		this.serverTs = serverTs;
	}

	public String getServer() {
		return server;
	}
	public void setServer(String server) {
		this.server = server;
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
