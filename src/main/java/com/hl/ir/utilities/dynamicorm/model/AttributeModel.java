package com.hl.ir.utilities.dynamicorm.model;

import java.util.List;
import java.util.Map;

public class AttributeModel {

	private String type;
	private List<String> keys;
	private String column;
	private Boolean jsonb;
	private Map<String, AttributeModel> attributes;
//	private String on;
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public List<String> getKeys() {
		return keys;
	}
	public void setKeys(List<String> keys) {
		this.keys = keys;
	}
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	public Boolean getJsonb() {
		return jsonb;
	}
	public void setJsonb(Boolean jsonb) {
		this.jsonb = jsonb;
	}
//	public String getOn() {
//		return on;
//	}
//	public void setOn(String on) {
//		this.on = on;
//	}
	public Map<String, AttributeModel> getAttributes() {
		return attributes;
	}
	public void setAttributes(Map<String, AttributeModel> attributes) {
		this.attributes = attributes;
	}

}
