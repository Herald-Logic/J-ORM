package com.hl.ir.utilities.dynamicorm;

import java.sql.Connection;
import java.util.Map;

import com.hl.ir.utilities.dynamicorm.builder.ORMSchemaBuilder;
import com.hl.ir.utilities.dynamicorm.model.JsonSchemaModel;

public abstract class ORMSchema {
	
	protected Map<String, JsonSchemaModel> schema;

	public abstract Object fetch(Connection connection, Object request, Object where);
	public abstract Object insert(Connection connection, Object request);
	public abstract int update(Connection connection, Object request);
	public abstract int delete(Connection connection, Object request);
	public abstract String validate(Object requestJson);
	
	public Object fetch(Object request, Object where) {
		return fetch(null, request, where);
	}
	
	public Object insert(Object request) {
		return insert(null, request);
	}
	
	public Object update(Object request) {
		return update(null, request);
	}
	
	public Object delete(Object request) {
		return delete(null, request);
	}
	
	public Map<String, JsonSchemaModel> getSchema() {
		return schema;
	}
	
	public static ORMSchemaBuilder builder() {
		return new ORMSchemaBuilder();
	}
}
