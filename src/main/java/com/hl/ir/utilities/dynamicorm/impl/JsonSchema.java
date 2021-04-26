package com.hl.ir.utilities.dynamicorm.impl;

import static com.hl.ir.utilities.db.sqlbuilder.query.Condition.condition;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import com.hl.ir.utilities.db.sqlbuilder.query.Update;
import com.hl.ir.utilities.db.sqlbuilder.query.Where;
import com.hl.ir.utilities.db.sqlbuilder.query.builder.UpdateBuilder;
import com.hl.ir.utilities.db.sqlbuilder.query.exception.QueryException;
import com.hl.ir.utilities.dynamicorm.DatabaseInteractor;
import com.hl.ir.utilities.dynamicorm.ORMSchema;
import com.hl.ir.utilities.dynamicorm.Validator;
import com.hl.ir.utilities.dynamicorm.builder.ORMSchemaBuilder;
import com.hl.ir.utilities.dynamicorm.model.AttributeModel;
import com.hl.ir.utilities.dynamicorm.model.DbInteractorModel;
import com.hl.ir.utilities.dynamicorm.model.JsonSchemaModel;
import com.hl.ir.utilities.dynamicorm.model.Properties;
import com.hl.ir.utilities.dynamicorm.model.RtbObject;
import com.hl.util.schema.json.exception.InvalidSchemaException;
import com.hl.util.schema.json.util.Constants;
import com.hl.utilities.db.DBType;

public class JsonSchema extends ORMSchema {

	DbInteractorModel dbOperationModel;
	Gson gson;

	public JsonSchema(ORMSchemaBuilder ormSchemaBuilder) {
		this.schema = ormSchemaBuilder.getSchema();
		GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeAdapter(new TypeToken<Map <String, Object>>(){}.getType(),  new MapDeserializerDoubleAsIntFix());
		gson = gsonBuilder.create();
		validateSchemaStructure(this.schema);
		dbOperationModel = createDbInteractorModel();
		System.out.println(gson.toJson(dbOperationModel));
	}

	private DbInteractorModel createDbInteractorModel() {
		dbOperationModel = new DbInteractorModel();
		RtbObject root = new RtbObject();
		Map<String, RtbObject> nonroots = new HashMap<String, RtbObject>();
		Map<String, JsonSchemaModel> schema = this.schema;
		for(String key: schema.keySet()) {
			JsonSchemaModel model = schema.get(key);
			if(model.getRoot() != null && model.getRoot()) {
				//form the root object
				setRtbObject(root, key, model);
				setExternalReferences(root, key, model);
			}
			else {
				//form the non root object
				RtbObject nonroot = new RtbObject();
				setRtbObject(nonroot, key, model);
				nonroots.put(key, nonroot);
			}
		}
		root.setNonroots(nonroots);
		dbOperationModel.setRtbObject(root);
		return dbOperationModel;
	}

	private void setExternalReferences(RtbObject root, String key, JsonSchemaModel model) {
		Map<String, RtbObject> references = new HashMap<String, RtbObject>();
		Map<String, JsonSchemaModel> schemaReferences = model.getReferences();
		for(String reference: schemaReferences.keySet()) {
			RtbObject referenceObject = new RtbObject();
			setRtbObject(referenceObject, reference, schemaReferences.get(reference));
			references.put(reference, referenceObject);
		}
		root.setExternalReferences(references);
	}

	private void setRtbObject(RtbObject root, String key, JsonSchemaModel model) {
		Map<String, AttributeModel> attributes = new HashMap<String, AttributeModel>();
		Map<String, AttributeModel> internalReferences = new HashMap<String, AttributeModel>();
		root.setObjectId(key);
		Map<String, Properties> properties = null;
		root.setRoot(model.getRoot());
		root.setInput(model.getInput());
		root.setOutput(model.getOutput());
		root.setPrimary(model.getPrimary());
		if(model.getProperties() != null) {
			properties = model.getProperties();
		} else if (model.getItems() != null) {
			root.setMultiple(true);
			JsonSchemaModel items = model.getItems();
			properties = items.getProperties();
		} 
		//		else {
		//			throw validation error
		//		}
		if(properties != null) {
			parseProperties(root, properties, attributes, internalReferences);
		}
		root.setAttributes(attributes);
		root.setInternalReferences(internalReferences);
	}

	private void parseProperties(RtbObject root, Map<String, Properties> properties, Map<String, AttributeModel> attributes, Map<String, AttributeModel> internalReferences) {
		for(String property : properties.keySet()) {
			Properties propertyObject = properties.get(property);
			if(property.equals("id"))
				root.setOuid(propertyObject.getReferenceId());
			AttributeModel attributeModel = new AttributeModel();
			attributeModel.setKeys(propertyObject.getKeys());
			attributeModel.setColumn(propertyObject.getColumn());
			attributeModel.setType(propertyObject.getType());
			attributeModel.setJsonb(propertyObject.getJsonb());
			if(properties.get(property).getOn() != null) {
				//					attributeModel.setOn(propertyObject.getOn());
				internalReferences.put(property, attributeModel);
			}
			else {
				Map<String, Properties> innerProperties = null;
				if(propertyObject.getProperties() != null) 
					innerProperties = propertyObject.getProperties();
				if(propertyObject.getItems() != null) 
					innerProperties = propertyObject.getItems().getProperties();
				if(innerProperties != null) {
					Map<String, AttributeModel> innerAttributes = new HashMap<String, AttributeModel>();
					attributeModel.setAttributes(new HashMap<String, AttributeModel>());
					parseProperties(root, innerProperties, innerAttributes, internalReferences);
					attributeModel.setAttributes(innerAttributes);
				}
				attributes.put(property, attributeModel);
			}
		}
	}

	private void validateSchemaStructure(Map<String, JsonSchemaModel> schemaMap) {
		for(String key: schemaMap.keySet()) {
			JsonSchemaModel schema = schemaMap.get(key);
			if(schema.getType() != null && schema.getType().equals(Constants.JSONSCHEMA_TYPE_OBJECT)) {
				if(schema.getProperties() == null)
					throw new InvalidSchemaException("Missing 'properties' for " + key);
			}
			else if(schema.getType() != null && schema.getType().equals(Constants.JSONSCHEMA_TYPE_ARRAY)) {
				if(schema.getItems() == null)
					throw new InvalidSchemaException("Missing 'items' for " + key);
				schema = schema.getItems();
				if(schema.getProperties() == null)
					throw new InvalidSchemaException("Missing 'properties' for " + key);
			}
			else if(schema.getType() != null && schema.getType().equals(Constants.JSONSCHEMA_TYPE_ARRAY_OBJECT)) {
				if(schema.getItems() == null)
					throw new InvalidSchemaException("Missing 'items' for " + key);
				if(schema.getItems().getProperties() == null)
					throw new InvalidSchemaException("Missing 'properties' for " + key);
				//			if(schema.has("objectKey")) // || !properties.has(schema.get("objectKey").getAsString()))
				//				throw new InvalidSchemaException("Invalid Object-Key for " + key);
				//			schema = schema.getItems();
			}
			//		if(!properties.has(Constants.JSONSCHEMA_PROPERTY_ID))
			//			throw new InvalidSchemaException("Missing mandatory property '"+ Constants.JSONSCHEMA_PROPERTY_ID +"'");
		}
	}

	@Override
	public JsonElement fetch(Connection connection, Object requestJson, Object whereJson) {
		try{
			JsonObject on = validateWhereJson(whereJson, null);
			((JsonObject)whereJson).add("on", on);
			dbOperationModel.setWhere(gson.fromJson(gson.toJson(whereJson), new TypeToken<Map<String, Object>>(){}.getType()));
			dbOperationModel.setSelect(gson.fromJson(gson.toJson(requestJson), new TypeToken<Map<String, Object>>(){}.getType()));
			System.out.println(gson.toJson(dbOperationModel));
			// Pass that to the dbinteractor method
			DatabaseInteractor dbui = DatabaseInteractor.getDatabaseInteractor(connection);
			return gson.fromJson(gson.toJson(dbui.select(dbOperationModel)), JsonElement.class); 
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private JsonObject validateWhereJson(Object whereJson, JsonObject on) {
		JsonObject request = (JsonObject) whereJson;
		Map<String, JsonSchemaModel> schema = this.schema;
		for(String key : request.keySet()) {
			if(key.equalsIgnoreCase("AND") || key.equalsIgnoreCase("OR")) {
				on = validateWhereJson(request.get(key), on);
			} else {
				for(String schemaKey: schema.keySet()) {
					String[] keys = key.split("[.]");
					String table = keys[0];
					String columns = keys[1];
					for(int i=2; i<keys.length; i++)
						columns += "." + keys[i];
					if(schemaKey.equalsIgnoreCase(table)) {
						Map<String, Properties> properties = null;
						if(schema.get(schemaKey).getProperties() != null) 
							properties = schema.get(schemaKey).getProperties();
						else if(schema.get(schemaKey).getItems() != null)
							properties = schema.get(schemaKey).getItems().getProperties();
						if(properties != null) {
							if(columns.contains(".")) {
								columns = table + columns.split("[.]")[0];
								if(properties.get(columns) != null && (properties.get(columns).getClause() == null || !properties.get(columns).getClause())) 
									System.out.println("throw validation exception");
							} else {
								if(properties.get(columns) != null && (properties.get(columns).getClause() == null || !properties.get(columns).getClause())) 
									System.out.println("throw validation exception");
							}
						}
						break;
					}
				}
			}
		}
		if(on == null) {
			on = new JsonObject();
			for(String schemaKey: schema.keySet()) {
				Map<String, Properties> properties = null;
				if(schema.get(schemaKey).getProperties() != null) 
					properties = schema.get(schemaKey).getProperties();
				else if(schema.get(schemaKey).getItems() != null)
					properties = schema.get(schemaKey).getItems().getProperties();
				if(properties != null) {
					for(String propertyKey: properties.keySet()) {
						if(properties.get(propertyKey).getOn() != null)
							on.addProperty(schemaKey+"."+propertyKey, properties.get(propertyKey).getOn());
					}
				}
			}
		}
		return on;
	}
	
	@Override
	public JsonElement insert(Connection connection, Object requestJson) {
		try {
			JsonObject insertJson = (JsonObject) requestJson;
			validateInsertJson(insertJson);
			dbOperationModel.setInsert(gson.fromJson(gson.toJson(insertJson), new TypeToken<Map<String, Object>>(){}.getType()));
			System.out.println(gson.toJson(dbOperationModel));
			// Pass that to the dbinteractor method
			DatabaseInteractor dbui = DatabaseInteractor.getDatabaseInteractor(connection);
			return gson.fromJson(gson.toJson(dbui.insert(dbOperationModel)), JsonElement.class); 
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private void validateInsertJson(JsonObject insertJson) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int update(Connection connection, Object requestJson) {
		JsonObject request= (JsonObject) requestJson;
		System.out.println(request.getClass());
		System.out.println(request);
		System.out.println(new Gson().toJson(dbOperationModel));
		Map<String, Object> requestMap= new HashMap<> ();
		requestMap.put("request", request);
		dbOperationModel.setUpdate(requestMap);
		DatabaseInteractor dbui= DatabaseInteractor.getDatabaseInteractor(connection);
		int counter= dbui.update(dbOperationModel);
		return counter;
	}

	@Override
	public int delete(Connection connection, Object requestJson) {
		return 0;
	}

	@Override
	public String validate(Object requestJson) {
		try {
			String validationResult= null;
			for(String key: schema.keySet()) {
				Validator validator = Validator.getValidator(schema.get(key), requestJson);
				validationResult = validator.performValidation();
			}
			return validationResult;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static class MapDeserializerDoubleAsIntFix implements JsonDeserializer<Map<String, Object>>{

		@Override  @SuppressWarnings("unchecked")
		public Map<String, Object> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
			return (Map<String, Object>) read(json);
		}

		public Object read(JsonElement in) {

			if(in.isJsonArray()){
				List<Object> list = new ArrayList<Object>();
				JsonArray arr = in.getAsJsonArray();
				for (JsonElement anArr : arr) {
					list.add(read(anArr));
				}
				return list;
			}else if(in.isJsonObject()){
				Map<String, Object> map = new LinkedTreeMap<String, Object>();
				JsonObject obj = in.getAsJsonObject();
				Set<Map.Entry<String, JsonElement>> entitySet = obj.entrySet();
				for(Map.Entry<String, JsonElement> entry: entitySet){
					map.put(entry.getKey(), read(entry.getValue()));
				}
				return map;
			}else if( in.isJsonPrimitive()){
				JsonPrimitive prim = in.getAsJsonPrimitive();
				if(prim.isBoolean()){
					return prim.getAsBoolean();
				}else if(prim.isString()){
					return prim.getAsString();
				}else if(prim.isNumber()){
					Number num = prim.getAsNumber();
					// here you can handle double int/long values
					// and return any type you want
					// this solution will transform 3.0 float to long values
					if(Math.ceil(num.doubleValue())  == num.longValue())
						return num.longValue();
					else{
						return num.doubleValue();
					}
				}
			}
			return null;
		}
	}

}
