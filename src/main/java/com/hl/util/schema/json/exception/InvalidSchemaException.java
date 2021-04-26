package com.hl.util.schema.json.exception;

public class InvalidSchemaException extends RuntimeException {
	private static final long serialVersionUID = 4196144221959006615L;
	
	public InvalidSchemaException(String message) {
		super(message);
	}
	public InvalidSchemaException(String message, Throwable throwable) {
		super(message,throwable);
	}
}
