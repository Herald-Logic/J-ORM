package com.wlh.core.schema.parser.model;

import java.util.Map;

import org.json.JSONObject;

public class ValueValidatorAndManipulator {
	
	private String type;
	private String value;
	private Map<String, Object> params;
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public Map<String, Object> getParams() {
		return params;
	}
	public void setParams(Map<String, Object> params) {
		this.params = params;
	}
	
	@Override
	public String toString() {
		return "[type=" + type + ", value=" + value + ", params=" + params + "]";
	}
}
