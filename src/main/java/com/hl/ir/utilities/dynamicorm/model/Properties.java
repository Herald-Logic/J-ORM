package com.hl.ir.utilities.dynamicorm.model;

import java.util.List;
import java.util.Map;

public class Properties {

	private String type;
	private String column;
	private Boolean clause;
	private String referenceId;
	private Boolean jsonb;
	private List<String> keys;
	private String on;
	private Map<String, Properties> properties;
	private JsonSchemaModel items;
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	public Boolean getClause() {
		return clause;
	}
	public void setClause(Boolean clause) {
		this.clause = clause;
	}
	public String getReferenceId() {
		return referenceId;
	}
	public void setReferenceId(String referenceId) {
		this.referenceId = referenceId;
	}
	public Boolean getJsonb() {
		return jsonb;
	}
	public void setJsonb(Boolean jsonb) {
		this.jsonb = jsonb;
	}
	public List<String> getKeys() {
		return keys;
	}
	public void setKeys(List<String> keys) {
		this.keys = keys;
	}
	public String getOn() {
		return on;
	}
	public void setOn(String on) {
		this.on = on;
	}
	public Map<String, Properties> getProperties() {
		return properties;
	}
	public void setProperties(Map<String, Properties> properties) {
		this.properties = properties;
	}
	public JsonSchemaModel getItems() {
		return items;
	}
	public void setItems(JsonSchemaModel items) {
		this.items = items;
	}
	
}
