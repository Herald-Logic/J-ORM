package com.wlh.core.schema.parser.model;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ReferenceId {
	private String referenceKey;
	private Object value;
//	private List<String> whereColumns = new ArrayList<>();
	
	
	private Map<String,Set<String>> whereColumns = new HashMap<>();
	
	public ReferenceId(){}
	
	public ReferenceId(ReferenceId referenceId){
		this.referenceKey = referenceId.referenceKey;
		this.value = referenceId.value;
		//No need to copy whereColumns as whereColumns for each level of referenceId would be different
	}
	
	
	
	public String getReferenceKey() {
		return referenceKey;
	}
	public ReferenceId setReferenceKey(String referenceKey) {
		this.referenceKey = referenceKey;
		return this;
	}
	public Object getValue() {
		return value;
	}
	public ReferenceId setValue(Object value) {
		this.value = value;
		return this;
	}
	public Set<String> getWhereColumns(String tableName) {
		if(whereColumns.get(tableName) == null)
			return null;
		return whereColumns.get(tableName);
	}
	public ReferenceId addWhereColumn(String tableName, String columnName) {
		if(whereColumns.get(tableName) == null)
			whereColumns.put(tableName, new HashSet<>());
		whereColumns.get(tableName).add(columnName);
		return this;
	}
	
	@Override
	public String toString() {
		return "[referenceKey=" + referenceKey + ", value=" + value + ", whereColumns=" + whereColumns
				+ "]";
	}
}
