package com.hl.ir.utilities.dynamicorm.impl;

import java.util.List;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.hl.ir.utilities.dynamicorm.Validator;
import com.hl.ir.utilities.dynamicorm.model.JsonSchemaModel;
import com.hl.util.schema.json.exception.SchemaValidationException;

public class JsonSchemaValidator extends Validator {

	public JsonSchemaValidator(JsonSchemaModel schemaModel, Object request) {
		this.schema = schemaModel;
		this.request = request;
	}

	@Override
	public String performValidation() throws SchemaValidationException {
		String valid = "success";
		
		try {
			JsonSchemaModel schemaElement = this.schema;
			JSONObject schema = new JSONObject(new Gson().toJson(schemaElement));
			JsonElement requestElement = (JsonElement) this.request;
			Schema jsonSchema = SchemaLoader.load(schema);
			jsonSchema.validate(new JSONObject(new Gson().toJson(requestElement)));
		} catch (org.everit.json.schema.ValidationException e) {
			List<String> errMsgStream = e.getAllMessages();
			valid = e.getAllMessages().toString();
			throw new SchemaValidationException(errMsgStream.toString(), e);
		}
		
		return valid;
	}
	
}
