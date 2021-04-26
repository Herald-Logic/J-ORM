package com.wlh.core.schema.parser.model;

public class JSONSchemaResponse {
	private String responseKey;
	private String referenceKey;
	
	//The format is used in case of date as currenty type=date is not supported in validation.
	//Should be removed in future when validation is done not by 3rd party.
	private String format;
	public String getFormat() {
		return format;
	}
	public JSONSchemaResponse setFormat(String format) {
		this.format = format;
		return this;
	}
	//remove till here
	
	public String getResponseKey() {
		return responseKey;
	}
	public JSONSchemaResponse setResponseKey(String responseKey) {
		this.responseKey = responseKey;
		return this;
	}
	public String getReferenceKey() {
		return referenceKey;
	}
	public JSONSchemaResponse setReferenceKey(String referenceKey) {
		this.referenceKey = referenceKey;
		return this;
	}
	@Override
	public String toString() {
		return "Response [responseKey=" + responseKey + ", referenceKey=" + referenceKey + "]";
	}
}
