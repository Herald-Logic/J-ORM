package com.hl.util.schema.json.exception;

public class SchemaProcessingException extends Exception{
	private static final long serialVersionUID = -6924545465981521120L;
	
	public SchemaProcessingException(String message) {
		super(message);
	}
	public SchemaProcessingException(String message, Throwable throwable) {
		super(message,throwable);
	}
}
