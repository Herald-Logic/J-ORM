package com.hl.ir.utilities.dynamicorm.model;

import java.util.Map;

public class JsonSchemaModel {
	private String type;
	private Boolean root;
	private Boolean input;
	private Boolean output;
	private Boolean primary;
	private Boolean reference;
	private Map<String, Properties> properties;
	private JsonSchemaModel items;
	private Map<String, JsonSchemaModel> references;
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public Boolean getRoot() {
		return root;
	}
	public void setRoot(Boolean root) {
		this.root = root;
	}
	public Boolean getInput() {
		return input;
	}
	public void setInput(Boolean input) {
		this.input = input;
	}
	public Boolean getOutput() {
		return output;
	}
	public void setOutput(Boolean output) {
		this.output = output;
	}
	public Boolean getPrimary() {
		return primary;
	}
	public void setPrimary(Boolean primary) {
		this.primary = primary;
	}
	public Boolean getReference() {
		return reference;
	}
	public void setReference(Boolean reference) {
		this.reference = reference;
	}
	public JsonSchemaModel getItems() {
		return items;
	}
	public void setItems(JsonSchemaModel items) {
		this.items = items;
	}
	public Map<String, JsonSchemaModel> getReferences() {
		return references;
	}
	public void setReferences(Map<String, JsonSchemaModel> references) {
		this.references = references;
	}
	public void setProperties(Map<String, Properties> properties) {
		this.properties = properties;
	}
	public Map<String, Properties> getProperties() {
		return properties;
	}
}
