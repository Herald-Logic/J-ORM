package com.hl.util.schema.json.parser.validator;

import java.io.IOException;
import java.util.List;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

import com.hl.util.schema.json.exception.SchemaValidationException;
import com.hl.util.schema.json.loader.JsonSchemaLoader;

public class Validator {

	public static String inlineValidateJson(JSONObject jsonToValidate, String fileKey) throws IOException, SchemaValidationException {
		JSONObject jsonSchema = JsonSchemaLoader.getJsonSchema(fileKey);
		return inlineValidateJson(jsonToValidate, jsonSchema);
	}
	
	public static String inlineValidateJson(JSONObject jsonToValidate, JSONObject jsonSchema) throws SchemaValidationException{

		String valid = "success";
		
		try {
			Schema schema = SchemaLoader.load(jsonSchema);
			schema.validate(jsonToValidate);
		} catch (org.everit.json.schema.ValidationException e) {
			List<String> errMsgStream = e.getAllMessages();
			valid = e.getAllMessages().toString();
			throw new SchemaValidationException(errMsgStream.toString(), e);
		}
		
		return valid;
	}
	
}
