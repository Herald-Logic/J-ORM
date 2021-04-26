package com.hl.util.schema.json.parser.reader;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hl.util.schema.json.exception.SchemaProcessingException;
import com.hl.util.schema.json.util.Constants;
import com.hl.utilities.db.DBManager;
import com.hl.utilities.db.statement.Statement;
import com.hl.utilities.db.statement.StatementFactory;
import com.hl.utilities.db.statement.StatementType;
import com.hl.utilities.db.statement.StatementUtil;

public class PostPutRequestParser {

	private static Logger logger = LoggerFactory.getLogger(PostPutRequestParser.class);
	private static StatementFactory statementFactory = new StatementFactory();
	
	private static Integer idCounter = 0;
	
	public static void mapJsonToSchema(Map<String, Statement>jsonToSchemaMapping, Map<String, Object> referenceIdsMap, Object entity, JSONObject jsonSchema, Set<String> colNameVal, Map<String, Object> idsMap, ArrayList<String> parseErrors, String qualifiedSchemaKey, List<HashMap<String, Object>> processorList) throws SchemaProcessingException {

		logger.info("");

		JSONObject propertiesJson = null;
		String tabName = "";
		String colName = "";
		boolean isUpdate = false;
		
		if(jsonSchema.keySet().contains(Constants.JSONSCHEMA_PROPERTY_PROPERTIES))
			propertiesJson = (JSONObject) jsonSchema.get(Constants.JSONSCHEMA_PROPERTY_PROPERTIES);
		else
			propertiesJson = jsonSchema;
		
		JSONObject entityJson = (JSONObject)entity;
		Set<String> entityJsonKeys = entityJson.keySet();
		
		//Check if INSERT or UPDATE
		if(entityJson.has(Constants.JSONSCHEMA_PROPERTY_ID)) {
			isUpdate = true;
			if(propertiesJson.has(Constants.JSONSCHEMA_PROPERTY_ID) 
				&& 
				propertiesJson.getJSONObject(Constants.JSONSCHEMA_PROPERTY_ID).has(Constants.JSONSCHEMA_PROPERTY_REFERENCE_ID)) {
				referenceIdsMap.put(
					propertiesJson.getJSONObject(Constants.JSONSCHEMA_PROPERTY_ID).getString(Constants.JSONSCHEMA_PROPERTY_REFERENCE_ID), 
					entityJson.get(Constants.JSONSCHEMA_PROPERTY_ID) instanceof String ?
						entityJson.getString(Constants.JSONSCHEMA_PROPERTY_ID) : entityJson.getInt(Constants.JSONSCHEMA_PROPERTY_ID)
				);
			}
		}
		else {
			if(propertiesJson.has(Constants.JSONSCHEMA_PROPERTY_ID) && !entityJsonKeys.isEmpty()) {
				int newId =  (++idCounter);
				entityJson.put(Constants.JSONSCHEMA_PROPERTY_ID, "temp"+newId);
				referenceIdsMap.put(
					propertiesJson.getJSONObject(Constants.JSONSCHEMA_PROPERTY_ID).getString(Constants.JSONSCHEMA_PROPERTY_REFERENCE_ID), 
					"temp"+newId);
			}
		}
		
		//This loop will not do anything if the type of the attribute is anything other than atomic data types
		for(String key : propertiesJson.keySet()) {
			if(
				!(propertiesJson.getJSONObject(key).getString("type").equalsIgnoreCase("object")) 
					|| 
				!(propertiesJson.getJSONObject(key).getString("type").equalsIgnoreCase("array"))
			) {
				if(propertiesJson.getJSONObject(key).has("referenceId") && entityJson.has(key) && !entityJsonKeys.isEmpty()) {
					referenceIdsMap.put(key, entityJson.get(key));
				}
			}
		}
		
		Set<String> schemaDataKeys = new HashSet<String>();
		
		//Iterate over JSONObject
		for (String key : entityJsonKeys) {
			boolean isJsonBAttribute= false;
			
			if(propertiesJson.has(key)) {
				schemaDataKeys.add(key);
				
				//If key has value as a JSONObject, recursive call
				boolean isInstanceOfJsonObject= entityJson.get(key) instanceof JSONObject;
				boolean isInstanceOfJsonArray= entityJson.get(key) instanceof JSONArray;
				if (isInstanceOfJsonObject) {
					if (((JSONObject) propertiesJson.get(key)).has(Constants.JSONSCHEMA_PROPERTY_JSONB)) {
						isJsonBAttribute= ((JSONObject) propertiesJson.get(key)).getBoolean(Constants.JSONSCHEMA_PROPERTY_JSONB);
					}
					
					
					if (!isJsonBAttribute) {
						mapJsonToSchema(jsonToSchemaMapping, referenceIdsMap, entityJson.getJSONObject(key), propertiesJson.getJSONObject(key), colNameVal, idsMap, parseErrors, qualifiedSchemaKey+"."+key, processorList);
					}
				}
				//If key has value as a JSONArray, recursive call
				else if (isInstanceOfJsonArray) {
					if (((JSONObject) propertiesJson.get(key)).has(Constants.JSONSCHEMA_PROPERTY_JSONB)) {
						isJsonBAttribute= ((JSONObject) propertiesJson.get(key)).getBoolean(Constants.JSONSCHEMA_PROPERTY_JSONB);
					}
					
					if (!isJsonBAttribute) {
						int arrCounter=0;
						for (Object arrayElement : entityJson.getJSONArray(key)) {
							if (arrayElement instanceof JSONObject)
								mapJsonToSchema(jsonToSchemaMapping, referenceIdsMap, arrayElement, propertiesJson.getJSONObject(key).getJSONObject(Constants.JSONSCHEMA_PROPERTY_ITEMS), colNameVal, idsMap, parseErrors, qualifiedSchemaKey+key+"["+arrCounter+"].", processorList);
							arrCounter++;
						}
					}
				}

				if (isJsonBAttribute || !(isInstanceOfJsonArray || isInstanceOfJsonObject)) { //If key has atomic value, add to jsonToSchemaMapping object or the value must be pointing to a jsonb attribute
					
					//Check if the field has columns
					if(propertiesJson.getJSONObject(key).has(Constants.JSONSCHEMA_PROPERTY_COLUMNS)) {

						//Iterate over all columns for the current field
						for(Object colArrayElement : propertiesJson.getJSONObject(key).getJSONArray(Constants.JSONSCHEMA_PROPERTY_COLUMNS)){
							tabName = colArrayElement.toString().split("\\.")[0];
							colName = colArrayElement.toString().split("\\.")[1];
							
							String mapKey = tabName+"|"+entityJson.get(Constants.JSONSCHEMA_PROPERTY_ID);//"SericeRegistry|temp1"
							StatementType statementType = StatementType.INSERT;
							if(jsonToSchemaMapping.containsKey(mapKey))
								statementType = jsonToSchemaMapping.get(mapKey).getStatementType();
							else if(isUpdate) 
								statementType = StatementType.UPDATE;
							
							//StatementType will be insert
							
							Object value = entityJson.get(key); //input_domain_objects json
							HashMap<String, Object> processor = null;
							Map<String, Object> processorWhere = new HashMap<String, Object>();
							if(propertiesJson.getJSONObject(key).has(Constants.JSONSCHEMA_PROPERTY_VALIDATOR)) {
								validate(value, propertiesJson.getJSONObject(key).getJSONObject(Constants.JSONSCHEMA_PROPERTY_VALIDATOR), mapKey, statementType, referenceIdsMap, parseErrors, qualifiedSchemaKey+"."+key);
							}
							if(propertiesJson.getJSONObject(key).has(Constants.JSONSCHEMA_PROPERTY_PROCESSOR)) {
								processor = setProcessor(value, propertiesJson.getJSONObject(key), mapKey, statementType, referenceIdsMap);
							}
							
							//Temp Code for type: date until firstParty validator is implemented
							if(propertiesJson.getJSONObject(key).has("format")) {
								String date = "";
								String format = propertiesJson.getJSONObject(key).getString("format");
								try {
									date = StatementUtil.sqlTimeStamp(entityJson.getString(key), format);
								} catch (ParseException e) {
									throw new SchemaProcessingException("Invalid format " + format + " for key=" + key, e);
								}
								value=date;
							}
							
							//If table|id exists in jsonToSchemaMapping, add to statement.columnValueMap
							if(jsonToSchemaMapping.containsKey(mapKey)) {
								if(statementType == StatementType.INSERT)
									jsonToSchemaMapping.get(mapKey).insert(colName, value.toString());
								else
									jsonToSchemaMapping.get(mapKey).set(colName, value.toString());
								//set unique Ids for json arrays
								//if the schema has uniqueIds key, pass the value to UniqueIdsMap
								if(propertiesJson.getJSONObject(key).has(Constants.JSONSCHEMA_PROPERTY_JSONARRAYUNIQUEID))
									jsonToSchemaMapping.get(mapKey).setUniqueIds(colName, propertiesJson.getJSONObject(key).getJSONObject(Constants.JSONSCHEMA_PROPERTY_JSONARRAYUNIQUEID));
							}
							//If table|id does not exist in jsonToSchemaMapping, create new statement object and add to jsonToSchemaMapping 
							else {
								Statement newStatement = statementFactory.createStatement(statementType, tabName);
								if(isUpdate) 
									newStatement.set(colName, value.toString());
								else
									newStatement.insert(colName, value.toString());
								//set unique Ids for json arrays
								//if the schema has uniqueIds key, pass the value to UniqueIdsMap
								if(propertiesJson.getJSONObject(key).has(Constants.JSONSCHEMA_PROPERTY_JSONARRAYUNIQUEID))
									newStatement.setUniqueIds(colName, propertiesJson.getJSONObject(key).getJSONObject(Constants.JSONSCHEMA_PROPERTY_JSONARRAYUNIQUEID));
								jsonToSchemaMapping.put(mapKey, newStatement);
							}
							
							//Parsing Processor for field
							if(propertiesJson.has(Constants.JSONSCHEMA_PROPERTY_INTERNALCOLUMNS)) {
								JSONObject intColProperties = propertiesJson.getJSONObject(Constants.JSONSCHEMA_PROPERTY_INTERNALCOLUMNS).getJSONObject(Constants.JSONSCHEMA_PROPERTY_PROPERTIES);
								for(String intCol : intColProperties.keySet()) {

									JSONObject intColJson = intColProperties.getJSONObject(intCol);
									//If internalColumns are Reference Columns, add their values to jsonToSchemaMapping object 
									if(processor!=null && referenceIdsMap.containsKey(intCol))
									//if(	.containsKey(intCol))
									
									if(referenceIdsMap.containsKey(intCol) && processor!=null) {
										
										JSONArray refCols = intColJson.getJSONArray(Constants.JSONSCHEMA_PROPERTY_COLUMNS);
										for(Object refCol : refCols) {
											if(jsonToSchemaMapping.get(mapKey).getStatementType().equals(StatementType.INSERT)) {
												
												String columnName = refCol.toString().split("\\.")[1];
												processorWhere.put(columnName, referenceIdsMap.get(intCol));
											} else {
												
												String columnName = refCol.toString().split("\\.")[1];
													processorWhere.put(columnName, referenceIdsMap.get(intCol));

											}
										}
									}
								}
							}
							//Setting where clause for processor updates
							if(processor!=null) {
								if(propertiesJson.has(Constants.JSONSCHEMA_PROPERTY_ID) && propertiesJson.getJSONObject(Constants.JSONSCHEMA_PROPERTY_ID).has(Constants.JSONSCHEMA_PROPERTY_COLUMNS)) {
									JSONArray idColumns = propertiesJson.getJSONObject(Constants.JSONSCHEMA_PROPERTY_ID).getJSONArray(Constants.JSONSCHEMA_PROPERTY_COLUMNS);
									String idRefId = propertiesJson.getJSONObject(Constants.JSONSCHEMA_PROPERTY_ID).getString(Constants.JSONSCHEMA_PROPERTY_REFERENCE_ID);
									for(Object idColObj : idColumns) 
										if(idColObj.toString().split("\\.")[0].equalsIgnoreCase(tabName))
											processorWhere.put(idColObj.toString().split("\\.")[1], referenceIdsMap.get(idRefId));
								}
							}	
							
							//check for "internalColumns" in JSONSchema
							if(propertiesJson.has(Constants.JSONSCHEMA_PROPERTY_INTERNALCOLUMNS)) {
								for(String intCol : propertiesJson
														.getJSONObject(Constants.JSONSCHEMA_PROPERTY_INTERNALCOLUMNS)
														.getJSONObject(Constants.JSONSCHEMA_PROPERTY_PROPERTIES)
														.keySet()) {
									JSONObject intColJson = propertiesJson.getJSONObject(Constants.JSONSCHEMA_PROPERTY_INTERNALCOLUMNS)
																		.getJSONObject(Constants.JSONSCHEMA_PROPERTY_PROPERTIES)
																		.getJSONObject(intCol);
									
									//If internalColumns are Reference Columns, add their values to jsonToSchemaMapping object 
									if(referenceIdsMap.containsKey(intCol)) {
										JSONArray refCols = intColJson.getJSONArray(Constants.JSONSCHEMA_PROPERTY_COLUMNS);
										
										for(Object refCol : refCols) {
											if(jsonToSchemaMapping.get(mapKey)
													.getStatementType()
													.equals(StatementType.INSERT)) {
												String columnName = refCol.toString().split("\\.")[1];
												jsonToSchemaMapping.get(mapKey).insert(intCol+"|"+referenceIdsMap.get(intCol), 
																						columnName);
												
												if(!idsMap.containsKey(intCol+"|"+referenceIdsMap.get(intCol))) {
													idsMap.put((intCol+"|"+referenceIdsMap.get(intCol)).replaceAll("'", ""), 
																0);
													//idsMap.put(intCol, referenceIdsMap.get(intCol)).replaceAll("'", ""));
												}
											} else {
												String columnName = refCol.toString().split("\\.")[1];
												String uniqueIdentifier = mapKey+tabName+columnName+referenceIdsMap.get(intCol);
												if(!colNameVal.contains(uniqueIdentifier)) {
													jsonToSchemaMapping.get(mapKey).where(columnName, referenceIdsMap.get(intCol));
													colNameVal.add(uniqueIdentifier);
												}
											}
										}
									}
									//If internalColumns are Default Value columns, get the concerned default values and add to jsonToSchemaMapping object
									else {
										if(!intColJson.has(Constants.JSONSCHEMA_PROPERTY_DEFAULT)) {
											if(jsonToSchemaMapping.get(mapKey).getStatementType() == StatementType.INSERT) {
												jsonToSchemaMapping.get(mapKey).insert(intCol, "");
											} else {
												jsonToSchemaMapping.get(mapKey).set(intCol, "");
											}
										}
									}
								}
							}
							//Getting InternalColumn Default Values
							if(propertiesJson.has(Constants.JSONSCHEMA_PROPERTY_INTERNALCOLUMNS)) {
								for(String intCol : propertiesJson
														.getJSONObject(Constants.JSONSCHEMA_PROPERTY_INTERNALCOLUMNS)
														.getJSONObject(Constants.JSONSCHEMA_PROPERTY_PROPERTIES)
														.keySet()) {
									JSONObject intColJson = propertiesJson.getJSONObject(Constants.JSONSCHEMA_PROPERTY_INTERNALCOLUMNS)
																		.getJSONObject(Constants.JSONSCHEMA_PROPERTY_PROPERTIES)
																		.getJSONObject(intCol);
									
									
									
									//If internalColumns are Reference Columns, add their values to jsonToSchemaMapping object 
									if(!referenceIdsMap.containsKey(intCol)) {
										JSONArray refCols = intColJson.getJSONArray(Constants.JSONSCHEMA_PROPERTY_COLUMNS);
										
										
										for(Object refCol : refCols) {
											
											if(intColJson.has(Constants.JSONSCHEMA_PROPERTY_DEFAULT)) {
												
												JSONObject defaultJson = propertiesJson.getJSONObject(Constants.JSONSCHEMA_PROPERTY_INTERNALCOLUMNS).getJSONObject(Constants.JSONSCHEMA_PROPERTY_PROPERTIES);
												setDefaultValues(intCol, defaultJson, entityJson, referenceIdsMap, jsonToSchemaMapping, parseErrors, qualifiedSchemaKey, statementType);
											
											} else {
												if(jsonToSchemaMapping.get(mapKey).getStatementType() == StatementType.INSERT) {
													jsonToSchemaMapping.get(mapKey).insert(intCol, "");
												} else {
													jsonToSchemaMapping.get(mapKey).set(intCol, "");
												}
											}
										}
									}
								}
							}
							
							//Set Table specific id column for where clause
							for(Object arrElement : propertiesJson
														.getJSONObject(Constants.JSONSCHEMA_PROPERTY_ID)
														.getJSONArray(Constants.JSONSCHEMA_PROPERTY_COLUMNS)) {
								String uniqueIdentifier = mapKey
															+tabName
															+arrElement.toString().split("\\.")[1]
															+entityJson.get(Constants.JSONSCHEMA_PROPERTY_ID);
								if(arrElement
										.toString()
										.split("\\.")[0]
										.equals(tabName) 
									&& 
									!colNameVal.contains(uniqueIdentifier)) { 
									
									if(jsonToSchemaMapping.get(mapKey).getStatementType() == StatementType.UPDATE){
										jsonToSchemaMapping.get(mapKey).where(arrElement.toString().split("\\.")[1], entityJson.get(Constants.JSONSCHEMA_PROPERTY_ID));
										processorWhere.put(arrElement.toString().split("\\.")[1], entityJson.get(Constants.JSONSCHEMA_PROPERTY_ID));
									}
									colNameVal.add(uniqueIdentifier);
								}
							}
							
							if(processor!=null)
								logger.debug("processor: "+processor);
							if(processor!=null && !processorWhere.isEmpty())
								logger.debug("processorWhere: "+processorWhere);
							
							if(processor!=null && !processorWhere.isEmpty())
								processor.put("where", processorWhere);
							
							
							//Set Unique Column Name for that Table
							if((jsonToSchemaMapping.get(mapKey).getUniqueColumnName() == null 
								|| 
								jsonToSchemaMapping.get(mapKey).getUniqueColumnName().equalsIgnoreCase("")) 
									&& 
									propertiesJson.has(Constants.JSONSCHEMA_PROPERTY_ID) 
									&& 
									propertiesJson.getJSONObject(Constants.JSONSCHEMA_PROPERTY_ID).has(Constants.JSONSCHEMA_PROPERTY_COLUMNS) 
									&& 
									propertiesJson.getJSONObject(Constants.JSONSCHEMA_PROPERTY_ID).getJSONArray(Constants.JSONSCHEMA_PROPERTY_COLUMNS).length()>=1) {
								
								jsonToSchemaMapping.get(mapKey).setUniqueColumnName(propertiesJson.getJSONObject("id")
																						.getJSONArray(Constants.JSONSCHEMA_PROPERTY_COLUMNS)
																						.get(0).toString()
																						.split("\\.")[1]);
							}
							
							//Inserting Dummy Reference Values
							if(key.equalsIgnoreCase(Constants.JSONSCHEMA_PROPERTY_ID)) {
								
								if(jsonToSchemaMapping.get(mapKey).getStatementType() == StatementType.INSERT) {
									jsonToSchemaMapping.get(mapKey).insert(Constants.JSONSCHEMA_PROPERTY_ID, 
																			propertiesJson.getJSONObject(key)
																				.getString(Constants.JSONSCHEMA_PROPERTY_REFERENCE_ID)
																				+"|"
																				+entityJson.get(Constants.JSONSCHEMA_PROPERTY_ID).toString());
								} else {
									jsonToSchemaMapping.get(mapKey).set(Constants.JSONSCHEMA_PROPERTY_ID, 
																		propertiesJson.getJSONObject(key)
																			.getString(Constants.JSONSCHEMA_PROPERTY_REFERENCE_ID)
																			+"|"
																			+entityJson.get(Constants.JSONSCHEMA_PROPERTY_ID).toString());
								}
								if(!idsMap.containsKey(propertiesJson.getJSONObject(key).getString(Constants.JSONSCHEMA_PROPERTY_REFERENCE_ID)+"|"+entityJson.get(Constants.JSONSCHEMA_PROPERTY_ID).toString())) {
									idsMap.put((propertiesJson.getJSONObject(key)
											.getString(Constants.JSONSCHEMA_PROPERTY_REFERENCE_ID)
											+"|"
											+entityJson.get(Constants.JSONSCHEMA_PROPERTY_ID).toString()).replaceAll("'", ""), 
											0);
								}
							}
							if(processor!=null)
								processorList.add(processor);
						}	
					}
				}
			}
			else {
				logger.info("Invalid JSON Key: "+key);
				parseErrors.add(qualifiedSchemaKey+"."+key+": Invalid Json Key");
			}
		}
		if(!entityJsonKeys.isEmpty())			
			for(String key : propertiesJson.keySet()) 
				if(!schemaDataKeys.contains(key) && propertiesJson.getJSONObject(key).has("columns"))
					setDefaultValues(key, propertiesJson, entityJson, referenceIdsMap, jsonToSchemaMapping, parseErrors, qualifiedSchemaKey+"."+key, null);

	}
	
	private static void setDefaultValues(String key, JSONObject schemaJson, JSONObject loanAppJson, Map<String, Object> referenceIdsMap, Map<String, Statement>jsonToSchemaMapping, ArrayList<String> parseErrors, String qualifiedSchemaKey, StatementType statementType) throws SchemaProcessingException {
			
		JSONArray colArr = schemaJson.getJSONObject(key).getJSONArray("columns");
		for(Object colObj : colArr) {
			
			String tabName = colObj.toString().split("\\.")[0];
			String colName = colObj.toString().split("\\.")[1];
			String mapKey = tabName+"|"+loanAppJson.get(Constants.JSONSCHEMA_PROPERTY_ID);
			
			//adding statement if not exists for mapKey
			if(!jsonToSchemaMapping.containsKey(mapKey)) {
				if(statementType!=null) {
					Statement newStatement = statementFactory.createStatement(statementType, tabName);
					jsonToSchemaMapping.put(mapKey, newStatement);
				}
			}
			
			if(schemaJson.getJSONObject(key).has("default")) {
				Object value=null;
				JSONObject defaultJson = schemaJson.getJSONObject(key).getJSONObject("default");
				if(defaultJson.has("type") && defaultJson.has("value")) {
					if(defaultJson.getString("type").equalsIgnoreCase("value") && jsonToSchemaMapping.get(mapKey).getStatementType()==StatementType.INSERT) {
						value = defaultJson.get("value");
					} else if (defaultJson.getString("type").equalsIgnoreCase("method")) {
						
						JSONObject paramsJson = null;
						Map<String, Object> paramsMap = new HashMap<String, Object>();
						String[] methodFullNameArr = defaultJson.getString("value").split("\\.");
						
						if(defaultJson.has("params")) {
							paramsJson = defaultJson.getJSONObject("params");
							for(String defValKey : paramsJson.keySet()) {
								if(referenceIdsMap.containsKey(defValKey)) {
									paramsMap.put(defValKey, referenceIdsMap.get(defValKey));
								} else {
									paramsMap.put(defValKey, paramsJson.get(defValKey));
								}
							}
						}
						
						try {
							String className = methodFullNameArr[0];
							for(int i=1; i<(methodFullNameArr.length-1); i++) {
								className += "."+methodFullNameArr[i];
							}
							Object obj;
								obj = Class.forName(className).newInstance();
							
							String methodName = methodFullNameArr[methodFullNameArr.length-1];
							Method method = obj.getClass().getMethod(methodName, StatementType.class, Map.class);
								
							value = method.invoke(obj, jsonToSchemaMapping.get(mapKey).getStatementType(), paramsMap);							
						} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | 
								NoSuchMethodException | SecurityException | IllegalArgumentException | InvocationTargetException e) {
							throw new SchemaProcessingException("Error while Processing Schema", e);
						}
					} else {
						parseErrors.add(qualifiedSchemaKey+": Invalid default type for key");
					}
				} else {
					parseErrors.add(qualifiedSchemaKey+": Invald default block for key");
				}

				if(value != null) {
					if(jsonToSchemaMapping.get(mapKey).getStatementType() == StatementType.INSERT) {
						jsonToSchemaMapping.get(mapKey).insert(colName, value);
					} else {
						jsonToSchemaMapping.get(mapKey).set(colName, value);
					}
				}
			}
		}
		
	}
	
	private static void validate(Object value, JSONObject validator, String mapKey, StatementType statementType, Map<String, Object> referenceIdsMap, ArrayList<String> parseErrors, String qualifiedSchemaKey) throws SchemaProcessingException {
		
		String validatorValue = validator.getString("value");
		String[] methodFullNameArr = validatorValue.split("\\.");
		String className = methodFullNameArr[0];
		for(int i=1; i<(methodFullNameArr.length-1); i++) {
			className += "."+methodFullNameArr[i];
		}
		
		Map<String, Object> paramsMap = new HashMap<String, Object>();
		if(validator.has("params")) {
			JSONObject paramsJson = validator.getJSONObject("params");
			for(String defValKey : paramsJson.keySet()) {
				if(referenceIdsMap.containsKey(defValKey)) {
					paramsMap.put(defValKey, referenceIdsMap.get(defValKey));
				} else {
					paramsMap.put(defValKey, paramsJson.get(defValKey));
				}
			}
		}
		
		try {
			Object obj = Class.forName(className).newInstance();
			String methodName = methodFullNameArr[methodFullNameArr.length-1];
			Method method = obj.getClass().getMethod(methodName, StatementType.class, Map.class);
			paramsMap.put("value", value);
			method.invoke(obj, statementType, paramsMap);
		} catch(InvocationTargetException e) {
			parseErrors.add(qualifiedSchemaKey+": "+e.getCause().getMessage());
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | SecurityException e) {
			throw new SchemaProcessingException("Error while Processing Schema", e);
		}
		
	}

	private static HashMap<String, Object> setProcessor(Object value, JSONObject schemaBlock, String mapKey, StatementType statementType, Map<String, Object> referenceIdsMap) throws SchemaProcessingException {
		logger.debug("value: {} | schemaBlock {} | mapKey: {} | statementType: {} | referenceIdsMap: {}", new Object[] {value, schemaBlock, mapKey, statementType, referenceIdsMap});

		JSONObject processor = schemaBlock.getJSONObject(Constants.JSONSCHEMA_PROPERTY_PROCESSOR);
		
		String tableName = mapKey.split("\\|")[0];
		String processorName = processor.getString("value");
		String[] processorNameArr = processorName.split("\\.");
		String className = processorNameArr[0];
		for(int i=1; i<(processorNameArr.length-1); i++) {
			className += "."+processorNameArr[i];
		}
		
		Map<String, Object> paramsMap = new HashMap<String, Object>();
		if(processor.has("params")) {
			JSONObject paramsJson = processor.getJSONObject("params");
			for(String defValKey : paramsJson.keySet()) {
				if(referenceIdsMap.containsKey(defValKey)) {
					paramsMap.put(defValKey, referenceIdsMap.get(defValKey));
				} else {
					paramsMap.put(defValKey, paramsJson.get(defValKey));
				}
			}
		}
		paramsMap.put("value", value);

		try {
			Object invokerObj = Class.forName(className).newInstance();
			String methodName = processorNameArr[processorNameArr.length-1];
			Method method = invokerObj.getClass().getMethod(methodName, StatementType.class, Map.class);
			
			HashMap<String, Object> processorObj = new HashMap<String, Object>();
			processorObj.put("invoker", invokerObj);
			processorObj.put("method", method);
			processorObj.put("statementType", statementType);
			processorObj.put("params", paramsMap);
			processorObj.put("table", tableName);
			processorObj.put("mapKey", mapKey);
			if(schemaBlock.has(Constants.JSONSCHEMA_PROPERTY_COLUMNS)) {
				JSONArray columns = schemaBlock.getJSONArray(Constants.JSONSCHEMA_PROPERTY_COLUMNS);
				for(Object colObj : columns) 
					if(colObj.toString().split("\\.")[0].equalsIgnoreCase(tableName))
						processorObj.put("column", colObj.toString().split("\\.")[1]);
			}
			return processorObj;
			
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | SecurityException e) {
			throw new SchemaProcessingException("Error while Processing Schema", e);
		}
	}

	//Execute the Request
	public static void executeInsertUpdate(Object loanApp, JSONObject jsonSchema) throws SQLException, SchemaProcessingException {
		executeInsertUpdate(null, loanApp, jsonSchema);
	}
	
	public static void executeInsertUpdate(Connection connection, Object loanApp, JSONObject jsonSchema) throws SQLException, SchemaProcessingException {
		logger.info("loanAppJson: " + loanApp + " | jsonSchema" + jsonSchema);
		
		boolean closable = false;
		boolean isConnectionPassed= false;
		if (connection == null) {
			connection = DBManager.getInstance().getConnection();
			closable = true;
		} else {
			isConnectionPassed= true;
		}
		boolean isAutoCommit = connection.getAutoCommit();
		connection.setAutoCommit(false);
		
		//Map which stores refId|id and its corresponding query
		//keys once inserted in this map should not be re-entered
		Map<String, Statement> jsonToSchemaMapping = new LinkedHashMap<String, Statement>();
		
		//Map to store all referenceIds
		Map<String, Object> referenceIdsMap = new HashMap<String, Object>();
		
		//map of refId|id and its correspondnig id-value
		Map<String, Object> idsMap = new HashMap<String, Object>();
		
		Set<String> colNameVal = new HashSet<String>();
		
		//String to track the current executing jsonKey
		String qualifiedSchemaKey = "json.";
		
		//List of all validation errors which might have occurred during parsing
		ArrayList<String> parseErrors = new ArrayList<String>();
		
		//List of processors which need to be executed
		List<HashMap<String, Object>> processorList = new ArrayList<HashMap<String, Object>>();
		
		//Ids which need to be inserted in processor where clause
		Map<String, Integer> processorIds = new HashMap<String, Integer>();
		
		int id = 0;
		int updateCounter = 0;
		int insertCounter = 0;
		//int testCounter = 0;
		
		//Parse requestJson to form queries
		PostPutRequestParser.mapJsonToSchema(jsonToSchemaMapping, referenceIdsMap, loanApp, jsonSchema, colNameVal,
				idsMap, parseErrors, qualifiedSchemaKey, processorList);
		//If any validation errors exist, form a string and throw exception
		if(!parseErrors.isEmpty()) {
			String errorsString = parseErrors.get(0);
			for(int i=1; i< parseErrors.size(); i++) {
				errorsString = errorsString+" || "+parseErrors.get(i);
			}
			throw new InvalidParameterException(errorsString);
		}
		
		logger.debug("jsonToSchemaMapping: "+jsonToSchemaMapping);
		
		Map<String, Object> tempIdsMap = new HashMap<String, Object>();
		for(String key : idsMap.keySet()) {
			tempIdsMap.put(key.split("\\|")[0], key.split("\\|")[1]);
			tempIdsMap.put(key, key.split("\\|")[1]);
		}
		idsMap.putAll(tempIdsMap);
				
		try {
			
			for (String key : jsonToSchemaMapping.keySet()) {
				
				String referenceId = null;
				if (jsonToSchemaMapping.get(key).getColumnValueMap().containsKey(Constants.JSONSCHEMA_PROPERTY_ID)) {
					referenceId = jsonToSchemaMapping.get(key).getColumnValueMap()
							.remove(Constants.JSONSCHEMA_PROPERTY_ID).toString();
				}
				
				jsonToSchemaMapping.get(key).getColumnValueMap().remove(jsonToSchemaMapping.get(key).getUniqueColumnName());
		
				//essential for insert queries
				//removing dummy inserted value for referenceIds and replacing with value inside idsMap
				for (String refKey : idsMap.keySet()) {
					if (jsonToSchemaMapping.get(key).getColumnValueMap().containsKey(refKey)) {
						String colName = (String) jsonToSchemaMapping.get(key).getColumnValueMap().remove(refKey);
						jsonToSchemaMapping.get(key).getColumnValueMap().put(colName.replaceAll("'", ""),
								idsMap.get(refKey));
					}
				}
				
				if(referenceId!=null) {
					Statement statement = jsonToSchemaMapping.get(key);
					if (statement.getStatementType().equals(StatementType.INSERT)) {
						
						//essential for root object
						//removing temp value for refId
						Map<String, Object> colValMap = statement.getColumnValueMap(); 
						Set<String> cols = new HashSet<String>(colValMap.keySet());
						for(String col : cols) {
							String value = colValMap.get(col).toString(); 
							if(value.contains("temp")) {
								colValMap.remove(col);
							}
						}
						
						logger.info("INSERT QUERY: " + statement.toString());
						System.out.println(statement);
						
						id = StatementUtil.insert(connection, statement);
						insertCounter++;
						
						//UnComment for testing without db
						/*testCounter++;
						id=testCounter;*/
						
						//put actual value generated during insert in idsMap
						logger.info(key+": "+"id: " + id);
						String tempVal = referenceId.split("\\|")[1].replaceAll("'", "");
						for(String idKey : idsMap.keySet()) {
							if(idKey.contains(tempVal) && !idsMap.containsKey(tempVal))
								idsMap.put(idKey, id);
						}
						if(!idsMap.containsKey(tempVal)) {
							idsMap.put(tempVal, id);					
						}
						//put actual value generated during insert in processorIds
						processorIds.put(referenceId.split("\\|")[1].replaceAll("'", ""), id);
						JSONObject entityJson = (JSONObject)loanApp;
						entityJson.put(statement.getTableName()+"_generated_id", id);
						
					} else if (statement.getStatementType().equals(StatementType.UPDATE)) {
		
						//Recreation of where clause
						Map<String, Object> newWhere = new HashMap<String, Object>();
						for(String col : statement.getWhereColumns() ) {
							String colName = col.split("\\|\\$\\|")[0];
							String colValue = col.split("\\|\\$\\|")[2].replace("'", "");
							newWhere.put(colName, colValue);
						}
						statement.resetWhere();
						logger.debug("newWhere: "+newWhere);
						for(String whereCol : newWhere.keySet()) {
							if(idsMap.containsKey(newWhere.get(whereCol)))
								statement.where(whereCol, idsMap.get(newWhere.get(whereCol)));
							else
								statement.where(whereCol, newWhere.get(whereCol));
						}
						
						logger.info("UPDATE QUERY: " + statement.toString());
						System.out.println("UPDATE QUERY: " + statement.toString());
						StatementUtil.update(connection, statement);
						updateCounter++;
						
					}
				}
			}
			
			logger.info("INSERTS FIRED: " + insertCounter + " | UPDATES FIRED: " + updateCounter);
			
//			connection.commit();
			
			Map<String, Statement> processorResult = executeProcessor(processorList, processorIds);;
			executeProcessorUpdates(connection, processorResult);
			
		} catch (Exception e) {
			if (closable && connection != null) {
				closable = false;
			}
			connection.rollback();
			throw e;
		} finally {
			if(!isConnectionPassed && connection!=null) {
				if (isAutoCommit)
					connection.commit();
				connection.setAutoCommit(isAutoCommit);
				if (closable) 
					connection.close();
			}
		}
	
	}
	
	@SuppressWarnings("unchecked")
	private static Map<String, Statement> executeProcessor(List<HashMap<String, Object>> processorList, Map<String, Integer> processorIds) throws SchemaProcessingException {
		logger.debug("procecssorList: {} | processorIds: {}", new Object[] {processorList, processorIds});
		
		HashMap<String, Statement> processorResult = new HashMap<String, Statement>();
		for(HashMap<String, Object> processor : processorList) {
			
			Object invoker = processor.get("invoker");
			Method method = (Method)processor.get("method");
			HashMap<String, Object> paramsMap = (HashMap<String, Object>)processor.get("params");
			Object value = paramsMap.get("value");
			StatementType statementType = (StatementType)processor.get("statementType");
			String processorResultKey = (String) processor.get("mapKey");
			String table = (String) processor.get("table");
			String columnName = (String) processor.get("column");
			HashMap<String, Object> where = (HashMap<String, Object>)processor.get("where");
			
			Statement statement = null;
			try {
				Object response = method.invoke(invoker, statementType, paramsMap);
				if(!method.getReturnType().equals(Void.TYPE))
					value = response;
				logger.debug("processor response: "+response);
			} catch(InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
				throw new SchemaProcessingException("Error while Processing Schema", e);
			}
			
			if(processorResult.containsKey(processorResultKey))
				statement = processorResult.get(processorResultKey);
			else
				statement = statementFactory.createStatement(StatementType.UPDATE, table);
			
			statement.set(columnName, value);
			for(String whereCol : where.keySet()) {
				Object val = where.get(whereCol);
				if(processorIds.containsKey(where.get(whereCol))) {
					val = processorIds.get(where.get(whereCol));
				}
				statement.where(whereCol, val);
			}
			processorResult.put(processorResultKey, statement);
			
			logger.debug("ProcessorQuery: "+statement.toString());
		}
	
		return processorResult;
	}
	
	private static void executeProcessorUpdates(Connection connection, Map<String, Statement> processorResult) throws SQLException {
		logger.debug("processorResult: {}", new Object[] {processorResult});
		
		for(String mapKey : processorResult.keySet()) {
			Statement statement = processorResult.get(mapKey);
			
			logger.info("Processor Update: "+statement);
			
			StatementUtil.update(connection, statement); 
		}
	} 
}
