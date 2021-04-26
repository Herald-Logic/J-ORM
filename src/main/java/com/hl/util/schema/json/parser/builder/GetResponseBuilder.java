package com.hl.util.schema.json.parser.builder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hl.util.schema.json.exception.InvalidSchemaException;
import com.hl.util.schema.json.exception.SchemaProcessingException;
import com.hl.util.schema.json.util.Constants;
import com.hl.utilities.db.statement.Statement;
import com.hl.utilities.db.statement.StatementFactory;
import com.hl.utilities.db.statement.StatementType;
import com.hl.utilities.db.statement.StatementUtil;
import com.wlh.core.schema.parser.model.JSONSchema;
import com.wlh.core.schema.parser.model.JSONSchemaOutput;
import com.wlh.core.schema.parser.model.JSONSchemaResponse;
import com.wlh.core.schema.parser.model.ReferenceId;
import com.wlh.core.schema.parser.model.ValueValidatorAndManipulator;

public class GetResponseBuilder {
	
	private static Logger logger= LoggerFactory.getLogger(GetResponseBuilder.class);
	private static StatementFactory statementFactory = new StatementFactory();
	
	public static JSONSchemaOutput formSchemaOutput(String key, JSONSchema schema, Map<String, Object> referenceIdsMap) {
		List<ReferenceId> referenceIds = getReferenceIdFromMap(schema, referenceIdsMap);
		return formSchemaOutput(key, schema, referenceIds);
	}
	
	private static JSONSchemaOutput formSchemaOutput(String key, JSONSchema schema, List<ReferenceId> referenceIds) {
		JSONSchemaOutput schemaOutput = new JSONSchemaOutput();
		schemaOutput.setKey(key);
		schemaOutput.setType(schema.getType());
		schemaOutput.setObjectKey(schema.getObjectKey());
		
		if(referenceIds == null)
			referenceIds = new LinkedList<>();
		referenceIds = deepCloneList(referenceIds);
		referenceIds.addAll(getReferenceIdFromMap(schema, schema.getReferences()));
		schemaOutput.setReferenceIds(referenceIds);
		
		Set<Statement> statements = new LinkedHashSet<>();
		schemaOutput.setStatements(statements);
		
		
		Map<String, JSONSchema> properties = null;
		if(schema.getType() !=null && schema.getType().equals(Constants.JSONSCHEMA_TYPE_OBJECT)) {
			properties = schema.getProperties();
		}
		else if(schema.getType() !=null && schema.getType().equals(Constants.JSONSCHEMA_TYPE_ARRAY)) {
			if(schema.getItems() == null)
				throw new InvalidSchemaException("Missing 'items' for " + key);
			schema = schema.getItems();
			properties = schema.getProperties();
		}
		else if(schema.getType() !=null && schema.getType().equals(Constants.JSONSCHEMA_TYPE_ARRAY_OBJECT)) {
			if(schema.getItems() == null)
				throw new InvalidSchemaException("Missing 'items' for " + key);
			properties = schema.getItems().getProperties();
			if(properties == null)
				throw new InvalidSchemaException("Missing 'items.properties' for " + key);
			if(schema.getObjectKey() == null || properties.get(schema.getObjectKey()) == null)
				throw new InvalidSchemaException("Invalid Object-Key for " + key);
			schema = schema.getItems();
		}
		
		if(properties == null)
			throw new InvalidSchemaException("Missing 'properties' for " + key);
		
		if(!properties.containsKey(Constants.JSONSCHEMA_PROPERTY_ID))
			throw new InvalidSchemaException("Missing mandatory property '"+ Constants.JSONSCHEMA_PROPERTY_ID +"'");

		/* childProperties is collection of childOutputSchemas, we maintain this mainly to allow normal properties to add value to 
		 * referenceIds if exist and then start with children so as each child may get all the referenceIds declared in parent
		 */
		List<Map.Entry<String, JSONSchema>> childProperties = new ArrayList<>();
		
		for (Map.Entry<String, JSONSchema> propertyEntry : properties.entrySet()) {
			JSONSchema property = propertyEntry.getValue();
			boolean isJsonb= property.isJsonb();
			System.out.println("Is this a Jsonb property: " + isJsonb);
			
			if(!propertyEntry.getKey().equals(Constants.JSONSCHEMA_PROPERTY_INTERNALCOLUMNS)) {
				//if property type is object then add jsonschema output to child
				
				boolean isUserDefined= property.getType() != null && 
						(property.getType().equals(Constants.JSONSCHEMA_TYPE_OBJECT) 
								|| property.getType().equals(Constants.JSONSCHEMA_TYPE_ARRAY) 
								|| property.getType().equals(Constants.JSONSCHEMA_TYPE_ARRAY_OBJECT));
				
				if(!isJsonb && isUserDefined) {
					childProperties.add(propertyEntry);
				}
				
				if (!isUserDefined || isJsonb) {
//					System.out.println("property = " + propertyEntry.getKey());
					logger.debug("property = " + propertyEntry.getKey());
					if(property.getColumns() != null && property.getColumns().size() > 0) {
						for (String column : property.getColumns()) {
							String[] tableColumnSplit = column.split("\\.");
							String tableName = tableColumnSplit[0];
							String columnName = tableColumnSplit[1];
							boolean isExistingStatement = false;
							for (Statement statement : statements) {
								if(statement.getTableName().equals(tableName)) {
									statement.select(columnName);
									isExistingStatement = true;
									break;
								}
							}
							if(!isExistingStatement) {
								//Since statement was not already created, we create a new statement and add default attributes
								Statement statement = getNewSelectStatement(tableName, schema, referenceIds);
								//Add column of current key as select column
								statement.select(columnName);
								//Add order by clause for new statement if exist
								if(schema.getSort() != null && schema.getSort().get(tableName) != null) {
									for (Map<String, Object> sort : schema.getSort().get(tableName)) {
										Boolean desc = (Boolean) sort.get(Constants.JSONSCHEMA_PROPERTY_SORT_DESCENDING);
										desc = desc == null ? false : desc;
										statement.orderBy((String) sort.get(Constants.JSONSCHEMA_PROPERTY_SORT_PROPERTY), desc);
									}
								}
								//Add default condition - Not working currently
								if(schema.getCondition() != null && schema.getCondition().get(tableName) != null) {
									for (String condition : schema.getCondition().get(tableName)) {
										statement.where(condition);
									}
								}
								statements.add(statement);
							}
							schemaOutput.addColumnToResponse(tableName, columnName, propertyEntry.getKey(), 
									property.getReferenceId(), property.getFormat());
						}
					}
					//Store Processor which manipulates the values
					if(property.getProcessor() != null) {
//						System.out.println("Adding Processor : " + propertyEntry.getKey() + " -> " + property.getProcessor());
						logger.debug("Adding Processor : " + propertyEntry.getKey() + " -> " + property.getProcessor());
						schemaOutput.addProcessor(propertyEntry.getKey(), property.getProcessor());
					}
					//Store DefaultValues which are to be called if DB returns NULL for a key.
					if(property.getDefaultValue() != null) {
//						System.out.println("Adding Default Value : " + propertyEntry.getKey() + " -> " + property.getDefaultValue());
						logger.debug("Adding Default Value : " + propertyEntry.getKey() + " -> " + property.getDefaultValue());
						schemaOutput.addDefaultValue(propertyEntry.getKey(), property.getDefaultValue());
					}
					if(property.getReferenceId() != null && !property.getReferenceId().isEmpty()) {
						ReferenceId referenceId = schemaOutput.addReferenceId(property.getReferenceId());
						/*
						 * Commented the below line to avoid a blocks referenceId to be used as where in the same block's query.
						 * Otherwise it will add in where clause as <column name> = null. 	
						 */
						//addWhereClause(schema, referenceId);
					}
				}
			}
		}
		
		for (Map.Entry<String, JSONSchema> childProperty : childProperties) {
//			System.out.println("childProperty = " + childProperty.getKey());
			logger.debug("childProperty = " + childProperty.getKey());
			schemaOutput.addChildSchemaOutput(formSchemaOutput(childProperty.getKey(), childProperty.getValue(), referenceIds));
		}
		return schemaOutput;
	}

	public static Object formResponse(JSONSchemaOutput schemaOutput, Map<String, Object> referenceValueMap) throws SQLException, SchemaProcessingException {
		return formResponse(null, schemaOutput, referenceValueMap);
	}
	public static Object formResponse(Connection connection, JSONSchemaOutput schemaOutput, Map<String, Object> referenceValueMap) throws SQLException, SchemaProcessingException {
		Object response = null;
		JSONObject responseObject = new JSONObject();
		JSONArray responseArray = new JSONArray();
		
		List<ReferenceId> referenceIds = schemaOutput.getReferenceIds();
		Set<Statement> statements = schemaOutput.getStatements();
		
		Map<String,List<Map<String,Object>>> statementResults = getStatementResults(connection, statements, referenceIds, referenceValueMap);
		
		if(schemaOutput.getType().equalsIgnoreCase(Constants.JSONSCHEMA_TYPE_OBJECT)) {
			
			if(statementResults.isEmpty())
				return responseObject;
			response = getResponseJsonObject(connection, 0, statementResults, schemaOutput, referenceValueMap);
		}
		else if(schemaOutput.getType().equalsIgnoreCase(Constants.JSONSCHEMA_TYPE_ARRAY)) {
			//Assumption here is that number of rows returned from all the statements would be exactly same
			int numberOfRows = statementResults.entrySet().iterator().next().getValue().size();
			for (int rowNumber = 0; rowNumber < numberOfRows; rowNumber++) {
				responseArray.put(getResponseJsonObject(connection, rowNumber, statementResults, schemaOutput, referenceValueMap));
			}
			response = responseArray;
		}
		else if(schemaOutput.getType().equalsIgnoreCase(Constants.JSONSCHEMA_TYPE_ARRAY_OBJECT)) {
			//Assumption here is that number of rows returned from all the statements would be exactly same
			int numberOfRows = statementResults.entrySet().iterator().next().getValue().size();
			for (int rowNumber = 0; rowNumber < numberOfRows; rowNumber++) {
				JSONObject tempResponseObject = getResponseJsonObject(connection, rowNumber, statementResults, schemaOutput, referenceValueMap);
				responseObject.put(tempResponseObject.getString(schemaOutput.getObjectKey()), tempResponseObject);
			}
			response = responseObject;
		}
		return response;
	}
	
	private static  List<ReferenceId> deepCloneList(List<ReferenceId> referenceIds) {
		List<ReferenceId> clonedReferenceIds = new LinkedList<>();
		for (ReferenceId referenceId : referenceIds) {
			clonedReferenceIds.add(new ReferenceId(referenceId));
		}
		return clonedReferenceIds;
	}
	
	private static Statement getNewSelectStatement(String tableName, JSONSchema schema, List<ReferenceId> referenceIds) {
		Statement statement = statementFactory.createStatement(StatementType.SELECT, tableName);
		if(referenceIds != null) {
			for(ReferenceId referenceId : referenceIds) {
				addWhereClause(schema, referenceId);
			}
		}
		return statement;
	}
	
	private static void addWhereClause(JSONSchema schema, ReferenceId referenceId) {
		if(schema.getProperties() != null) {
			JSONSchema whereSchema = schema.getProperties().get(referenceId.getReferenceKey());
			if(whereSchema == null && schema.getProperties().get(Constants.JSONSCHEMA_PROPERTY_INTERNALCOLUMNS) != null) {
				whereSchema = schema.getProperties().get(Constants.JSONSCHEMA_PROPERTY_INTERNALCOLUMNS)
														.getProperties().get(referenceId.getReferenceKey());
			}
			if(whereSchema != null) {
				for(String whereColumn : whereSchema.getColumns()) {
					String[] whereTableColumnSplit = whereColumn.split("\\.");
					String whereColumnTableName = whereTableColumnSplit[0];
					String whereColumnName = whereTableColumnSplit[1];
//					if(tableName.equalsIgnoreCase(whereColumnTableName))
						referenceId.addWhereColumn(whereColumnTableName,whereColumnName);
				}
			}
		}
	}
	
	private static Map<String,List<Map<String,Object>>> getStatementResults(Connection connection, Set<Statement> statements, List<ReferenceId> referenceIds, Map<String, Object> referenceValueMap) throws SQLException {
		Map<String,List<Map<String,Object>>> statementResults = new HashMap<>();
		
		for (Statement statement : statements) {
			//Need to reset the where clause to avoid stale values while fetching child objects of array items.
			statement.resetWhere();
			for (ReferenceId referenceId : referenceIds) {
				Set<String> whereColumns = referenceId.getWhereColumns(statement.getTableName());
				if(whereColumns != null) {
					for (String whereColumn : whereColumns) {
						statement.where(whereColumn, referenceValueMap.get(referenceId.getReferenceKey()));
					}
				}
			}
//			System.out.println(statement);
			logger.debug(statement.toString());
			statementResults.put(statement.getTableName(), StatementUtil.select(connection, statement));
		}
		return statementResults;
	}
	
	private static JSONObject getResponseJsonObject(Connection connection, int rowNumber, Map<String,List<Map<String,Object>>> statementResults, JSONSchemaOutput schemaOutput, Map<String, Object> referenceValueMap) throws SQLException, SchemaProcessingException {
		JSONObject responseObject = new JSONObject();
		for (Map.Entry<String,List<Map<String,Object>>> statementResultEntry : statementResults.entrySet()) {
			List<Map<String,Object>> rows = statementResultEntry.getValue();
			if(rowNumber >= rows.size())
				continue;
			Map<String, Object> row = rows.get(rowNumber); 
			for (Map.Entry<String, Object> column : row.entrySet()) {
				JSONSchemaResponse response = schemaOutput.getResponse(statementResultEntry.getKey(),column.getKey());
				if(response == null)
					continue;
				
				//The format is used in case of date as currenty type=date is not supported in validation.
				//Should be removed in future when validation is done not by 3rd party.
				Object columnValue = column.getValue();
				if(response.getFormat() != null) {
					DateFormat df = new SimpleDateFormat(response.getFormat());
					try {
						columnValue = df.format(columnValue);
					}
					catch (Exception e) {
						logger.error("Not able to parse with specified format : Value = " + columnValue + ", Format = " + response.getFormat(),e);
						continue;
					}
				}
				//remove till here
				
				responseObject.put(response.getResponseKey(), columnValue);
				if(response.getReferenceKey() != null && !response.getReferenceKey().isEmpty())
					referenceValueMap.put(response.getReferenceKey(), columnValue);
			}
		}
		//Calling Processors
		for(Map.Entry<String, ValueValidatorAndManipulator> processorEntry : schemaOutput.getProcessors().entrySet()) {
			ValueValidatorAndManipulator processor = processorEntry.getValue();
			
//			System.out.println("Executing Processor : " + processorEntry.getKey() + " -> " + processor);
			logger.debug("Executing Processor : " + processorEntry.getKey() + " -> " + processor);
			Object originalValue = null;
			if(responseObject.has(processorEntry.getKey()))
				 originalValue = responseObject.get(processorEntry.getKey());
			
			Object processorValue = invokeMethod(processor, referenceValueMap, originalValue);
			if(processorValue != null)
				responseObject.put(processorEntry.getKey(), processorValue);
		}
		//Calling default values
		for(Map.Entry<String, ValueValidatorAndManipulator> defaultValuesEntry : schemaOutput.getDefaultValues().entrySet()) {
			//If value is already fetched from DB or Processor. No need of DefaultValue.
			if(responseObject.has(defaultValuesEntry.getKey()))
				continue;
			
			ValueValidatorAndManipulator defaultValue = defaultValuesEntry.getValue();
			
//			System.out.println("Executing Default Value : " + defaultValuesEntry.getKey() + " -> " + defaultValue);
			logger.debug("Executing Default Value : " + defaultValuesEntry.getKey() + " -> " + defaultValue);
			if(Constants.JSONSCHEMA_DEFAULTVALUE_TYPE_VALUE.equals(defaultValue.getType()))
				responseObject.put(defaultValuesEntry.getKey(), defaultValue.getValue());
			else
				responseObject.put(defaultValuesEntry.getKey(), invokeMethod(defaultValue, referenceValueMap));
		}
		//Building child elements response
		for (JSONSchemaOutput childSchemaOutput : schemaOutput.getChildSchemaOutput()) {
			responseObject.put(childSchemaOutput.getKey(), formResponse(connection, childSchemaOutput, referenceValueMap));
		}
		
		return responseObject;
	}
	
	private static List<ReferenceId> getReferenceIdFromMap(JSONSchema schema, Map<String, Object> referenceIdsMap) {
		List<ReferenceId> referenceIds = new LinkedList<>();
		if(referenceIdsMap != null) {
			for (Map.Entry<String, Object> referenceIdEntry : referenceIdsMap.entrySet()) {
				ReferenceId referenceId = new ReferenceId().setReferenceKey(referenceIdEntry.getKey()).setValue(referenceIdEntry.getValue());
				addWhereClause(schema, referenceId);
				referenceIds.add(referenceId);
			}
		}
		return referenceIds;
	}
	
	
	private static Object invokeMethod(ValueValidatorAndManipulator valueManipulator, Map<String, Object> referenceValueMap) throws SchemaProcessingException {
		return invokeMethod(valueManipulator, referenceValueMap, null);
	}
	private static Object invokeMethod(ValueValidatorAndManipulator valueManipulator, Map<String, Object> referenceValueMap, Object originalValue) throws SchemaProcessingException {
		
		String fullyQualifiedMethodName = valueManipulator.getValue();
		int delimiter = fullyQualifiedMethodName.lastIndexOf(".");
		String className = fullyQualifiedMethodName.substring(0, delimiter);
		String methodName = fullyQualifiedMethodName.substring(delimiter + 1);
		
		try {
			Object obj = Class.forName(className).newInstance();
			Method method = obj.getClass().getMethod(methodName, StatementType.class, Map.class);
			
			Map<String, Object> params = new LinkedHashMap<>();
			if(valueManipulator.getParams() != null) {
				params = valueManipulator.getParams();
				for(String param : params.keySet()) {
					if(referenceValueMap.containsKey(param))
						params.put(param, referenceValueMap.get(param));
				}
			}
			params.put(Constants.JSONSCHEMA_PROCESSOR_PARAMKEY_ORIGINAL_VALUE, originalValue);
			Object returnValue = method.invoke(obj, StatementType.SELECT, params);
			if(method.getReturnType().equals(Void.TYPE))
				returnValue = null;
			return returnValue;
			
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException 
				| SecurityException | IllegalArgumentException | InvocationTargetException e) {
			throw new SchemaProcessingException("Error while Processing Schema", e);
		}
	}
}
