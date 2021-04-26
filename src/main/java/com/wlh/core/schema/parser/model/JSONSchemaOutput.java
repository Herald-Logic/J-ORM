package com.wlh.core.schema.parser.model;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;

import com.hl.utilities.db.statement.Statement;

public class JSONSchemaOutput {
	private String key;
	private String type;
	private String objectKey;
	private Map<String, ValueValidatorAndManipulator> processors = new LinkedHashMap<>();
	private Map<String, ValueValidatorAndManipulator> defaultValues = new LinkedHashMap<>();
	private Set<Statement> statements = new LinkedHashSet<>();
	private Map<String,Map<String, JSONSchemaResponse>> tableToColumnResponseMap = new LinkedHashMap<>();
	private List<ReferenceId> referenceIds = new LinkedList<>();
	private List<JSONSchemaOutput> childSchemaOutput = new ArrayList<>();
	
	public String getKey() {
		return key;
	}
	public JSONSchemaOutput setKey(String key) {
		this.key = key;
		return this;
	}
	public String getType() {
		return type;
	}
	public JSONSchemaOutput setType(String type) {
		this.type = type;
		return this;
	}
	public String getObjectKey() {
		return objectKey;
	}
	public JSONSchemaOutput setObjectKey(String objectKey) {
		this.objectKey = objectKey;
		return this;
	}
	public Map<String, ValueValidatorAndManipulator> getProcessors() {
		return processors;
	}
	public JSONSchemaOutput setProcessors(Map<String, ValueValidatorAndManipulator> processors) {
		this.processors = processors;
		return this;
	}
	public Map<String, ValueValidatorAndManipulator> getDefaultValues() {
		return defaultValues;
	}
	public JSONSchemaOutput setDefaultValues(Map<String, ValueValidatorAndManipulator> defaultValues) {
		this.defaultValues = defaultValues;
		return this;
	}
	public List<ReferenceId> getReferenceIds() {
		return referenceIds;
	}
	public JSONSchemaOutput setReferenceIds(List<ReferenceId> referenceIds) {
		this.referenceIds = referenceIds;
		return this;
	}
	public Set<Statement> getStatements() {
		return statements;
	}
	public JSONSchemaOutput setStatements(Set<Statement> statements) {
		this.statements = statements;
		return this;
	}
	public List<JSONSchemaOutput> getChildSchemaOutput() {
		return childSchemaOutput;
	}
	public JSONSchemaOutput setChildSchemaOutput(List<JSONSchemaOutput> childSchemaOutput) {
		this.childSchemaOutput = childSchemaOutput;
		return this;
	}
	
	
	//Utility Methods
	public JSONSchemaOutput addChildSchemaOutput(JSONSchemaOutput childSchemaOutput) {
		this.childSchemaOutput.add(childSchemaOutput);
		return this;
	}
	public JSONSchemaOutput addColumnToResponse(String tableName, String columnName, String responseKey, String referenceKey, String format) {
		if(this.tableToColumnResponseMap.get(tableName) == null)
			this.tableToColumnResponseMap.put(tableName, new HashMap<>());
		JSONSchemaResponse response = new JSONSchemaResponse().setResponseKey(responseKey).setReferenceKey(referenceKey).setFormat(format);
		this.tableToColumnResponseMap.get(tableName).put(columnName, response);
		return this;
	}
	public JSONSchemaResponse getResponse(String tableName, String columnName) {
		if(this.tableToColumnResponseMap.get(tableName) == null)
			return null;
		return this.tableToColumnResponseMap.get(tableName).get(columnName);
	}
	
	public ReferenceId addReferenceId(String referenceKey) {
		return addReferenceId(referenceKey,null);
	}
	public ReferenceId addReferenceId(String referenceKey, Object value) {
		ReferenceId referenceId = new ReferenceId().setReferenceKey(referenceKey).setValue(value);
		this.referenceIds.add(referenceId);
		return referenceId;
	}
	
	public JSONSchemaOutput addProcessor(String key, ValueValidatorAndManipulator processor) {
		this.processors.put(key, processor);
		return this;
	}
	
	public JSONSchemaOutput addDefaultValue(String key, ValueValidatorAndManipulator defaultValue) {
		this.defaultValues.put(key, defaultValue);
		return this;
	}
	
	@Override
	public String toString() {
		JSONObject string = new JSONObject();
		string.put("key", key).put("type", type).put("processors", processors).put("defaultValues", defaultValues).put("referenceIds", referenceIds).put("statements", statements).put("columnResponseMap", tableToColumnResponseMap)
		.put("childSchemaOutput", childSchemaOutput);
		
		
//		return string.toString();
		return "[key="+key + ", type=" + type + ", processors=" + processors + ", defaultValues=" + defaultValues + ", referenceIds=" + referenceIds + ", statements=" + statements + ", columnResponseMap=" + tableToColumnResponseMap + ", childSchemaOutput=" + childSchemaOutput + "]\n";
	}
}
