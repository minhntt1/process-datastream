package com.example.datastream.model;

import java.util.Objects;

import com.google.protobuf.ByteString;

public class EdgeKey {
	private ByteString traceId;
	private ByteString spanId;
	public EdgeKey(ByteString traceId, ByteString spanId) {
		super();
		this.traceId = traceId;
		this.spanId = spanId;
	}
	public ByteString getTraceId() {
		return traceId;
	}
	public void setTraceId(ByteString traceId) {
		this.traceId = traceId;
	}
	public ByteString getSpanId() {
		return spanId;
	}
	public void setSpanId(ByteString spanId) {
		this.spanId = spanId;
	}
	@Override
	public int hashCode() {
		return Objects.hash(spanId, traceId);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EdgeKey other = (EdgeKey) obj;
		return Objects.equals(spanId, other.spanId) && Objects.equals(traceId, other.traceId);
	}
}
