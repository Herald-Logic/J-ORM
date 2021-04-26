package com.hl.util.schema.json.loader;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.json.JSONObject;

import com.hl.utils.properties.PropertyManager;

public class JsonSchemaLoader {

	private static final Map<String, JSONObject> jsonSchemaMap = new HashMap<String, JSONObject> ();
	
	public static JSONObject getJsonSchema(String schemaName) throws IOException {
		loadJsonSchema(schemaName);
		return jsonSchemaMap.get(schemaName);
	}
	
	public static void loadJsonSchema(String schemaName) throws IOException {
		
		if(!jsonSchemaMap.containsKey(schemaName)) {
			
			System.out.println("Schema "+schemaName+" not found in cache, Loading...");
			
			String absolutePath = JsonSchemaLoader.class.getClassLoader().getResource("json-schema").getPath();
			String fileName = PropertyManager.getParam(schemaName);
			System.out.println("filePath: "+absolutePath+"/"+fileName);
			try(FileReader file = new FileReader(absolutePath+"/"+fileName);
					Scanner sc = new Scanner(file);){
				JSONObject jsonSchema = new JSONObject(sc.nextLine());
				jsonSchemaMap.put(schemaName, jsonSchema);
			} catch(IOException e) {
				e.printStackTrace();
				throw e;
			}
		} else {
			System.out.println("Schema "+schemaName+" found in cache, Returning...");
		}
		
	}
	
	public static void clearCache() {
		jsonSchemaMap.clear();
	}
	
}
