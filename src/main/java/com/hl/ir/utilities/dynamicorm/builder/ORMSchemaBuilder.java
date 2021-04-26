package com.hl.ir.utilities.dynamicorm.builder;

import java.util.Map;

import com.hl.ir.utilities.dynamicorm.ORMSchema;
import com.hl.ir.utilities.dynamicorm.impl.JsonSchema;
import com.hl.ir.utilities.dynamicorm.model.JsonSchemaModel;

public class ORMSchemaBuilder {

	private Map<String, JsonSchemaModel> schema;
	
	public ORMSchema build() {
		return new JsonSchema(this);
	}
	
	public ORMSchemaBuilder schema(Map<String, JsonSchemaModel> schema) {
		this.schema = schema;
		return this;
	}

	public Map<String, JsonSchemaModel> getSchema() {
		return schema;
	}

}
