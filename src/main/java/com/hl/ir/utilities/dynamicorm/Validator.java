package com.hl.ir.utilities.dynamicorm;

import com.hl.ir.utilities.dynamicorm.impl.JsonSchemaValidator;
import com.hl.ir.utilities.dynamicorm.model.JsonSchemaModel;

public abstract class Validator {

	protected JsonSchemaModel schema;
	protected Object request;
	public abstract String performValidation() throws Exception;
	
	public static Validator getValidator(JsonSchemaModel schemaModel, Object request) {
		return new JsonSchemaValidator(schemaModel, request);
	}
}
