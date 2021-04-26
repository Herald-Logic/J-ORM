package com.wlh.core.schema.parser.model;

import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

public class JSONSchema {
	private String type;
	private String objectKey; // Used for type=ARRAY-OBJECT
	private boolean jsonb;
	@SerializedName("default")
	private ValueValidatorAndManipulator defaultValue;
	private ValueValidatorAndManipulator validator;
	private ValueValidatorAndManipulator processor;
	private String referenceId;
	private JSONSchema items;
	private List<String> columns;
	private Map<String,JSONSchema> properties;
	private Map<String, Object> references;
	private Map<String, List<String>> condition;
	//TableName -> List of columns and metadata to be used for sorting
	private Map<String, List<Map<String, Object>>> sort;
	
	//The format is used in case of date as currenty type=date is not supported in validation.
	//Should be removed in future when validation is done not by 3rd party.
	private String format;
	public String getFormat() {
		return format;
	}
	public JSONSchema setFormat(String format) {
		this.format = format;
		return this;
	}
	//remove till here
	
	public String getType() {
		return type;
	}
	public JSONSchema setType(String type) {
		this.type = type;
		return this;
	}
	public String getObjectKey() {
		return objectKey;
	}
	public JSONSchema setObjectKey(String objectKey) {
		this.objectKey = objectKey;
		return this;
	}
	public ValueValidatorAndManipulator getDefaultValue() {
		return defaultValue;
	}
	public JSONSchema setDefaultValue(ValueValidatorAndManipulator defaultValue) {
		this.defaultValue = defaultValue;
		return this;
	}
	public ValueValidatorAndManipulator getValidator() {
		return validator;
	}
	public JSONSchema setValidator(ValueValidatorAndManipulator validator) {
		this.validator = validator;
		return this;
	}
	public ValueValidatorAndManipulator getProcessor() {
		return processor;
	}
	public JSONSchema setProcessor(ValueValidatorAndManipulator processor) {
		this.processor = processor;
		return this;
	}
	public String getReferenceId() {
		return referenceId;
	}
	public JSONSchema setReferenceId(String referenceId) {
		this.referenceId = referenceId;
		return this;
	}
	public JSONSchema getItems() {
		return items;
	}
	public JSONSchema setItems(JSONSchema items) {
		this.items = items;
		return this;
	}
	public List<String> getColumns() {
		return columns;
	}
	public JSONSchema setColumns(List<String> columns) {
		this.columns = columns;
		return this;
	}
	public Map<String, JSONSchema> getProperties() {
		return properties;
	}
	public JSONSchema setProperties(Map<String, JSONSchema> properties) {
		this.properties = properties;
		return this;
	}
	public Map<String, Object> getReferences() {
		return references;
	}
	public void setReferences(Map<String, Object> references) {
		this.references = references;
	}
	public Map<String, List<String>> getCondition() {
		return condition;
	}
	public void setCondition(Map<String, List<String>> condition) {
		this.condition = condition;
	}
	public Map<String, List<Map<String, Object>>> getSort() {
		return sort;
	}
	public void setSort(Map<String, List<Map<String, Object>>> sort) {
		this.sort = sort;
	}
	public boolean isJsonb() {
		return jsonb;
	}
	public void setJsonb(boolean jsonb) {
		this.jsonb = jsonb;
	}
}
