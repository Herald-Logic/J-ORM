package com.hl.util.schema.json.exception;

public class SchemaValidationException extends Exception {

	private static final long serialVersionUID = 531675608778165511L;

	public SchemaValidationException(String message) {
		super(message);
	}
	
	public SchemaValidationException(String message, Throwable e) {
		super(message, e);
	}
	
}
