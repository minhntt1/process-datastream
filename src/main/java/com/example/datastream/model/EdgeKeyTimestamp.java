package com.example.datastream.model;
public class EdgeKeyTimestamp {
	private EdgeKey edgeKey;
	private long startTs;
	public EdgeKeyTimestamp(EdgeKey edgeKey, long startTs) {
		super();
		this.edgeKey = edgeKey;
		this.startTs = startTs;
	}
	public EdgeKey getEdgeKey() {
		return edgeKey;
	}
	public void setEdgeKey(EdgeKey edgeKey) {
		this.edgeKey = edgeKey;
	}
	public long getStartTs() {
		return startTs;
	}
	public void setStartTs(long startTs) {
		this.startTs = startTs;
	}
}
