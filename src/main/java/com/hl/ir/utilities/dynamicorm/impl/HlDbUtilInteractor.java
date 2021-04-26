package com.hl.ir.utilities.dynamicorm.impl;

import static com.hl.ir.utilities.db.sqlbuilder.query.Attribute.attribute;
import static com.hl.ir.utilities.db.sqlbuilder.query.Attribute.jsonb_agg;
import static com.hl.ir.utilities.db.sqlbuilder.query.Condition.condition;
import static com.hl.ir.utilities.db.sqlbuilder.query.Table.jsonb_array_elements;
import static com.hl.ir.utilities.db.sqlbuilder.query.Table.table;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.hl.ir.utilities.db.sqlbuilder.query.Attribute;
import com.hl.ir.utilities.db.sqlbuilder.query.Condition;
import com.hl.ir.utilities.db.sqlbuilder.query.Insert;
import com.hl.ir.utilities.db.sqlbuilder.query.Select;
import com.hl.ir.utilities.db.sqlbuilder.query.Table;
import com.hl.ir.utilities.db.sqlbuilder.query.Update;
import com.hl.ir.utilities.db.sqlbuilder.query.Where;
import com.hl.ir.utilities.db.sqlbuilder.query.WhereOperator;
import com.hl.ir.utilities.db.sqlbuilder.query.builder.UpdateBuilder;
import com.hl.ir.utilities.db.sqlbuilder.query.exception.QueryException;
import com.hl.ir.utilities.dynamicorm.DatabaseInteractor;
import com.hl.ir.utilities.dynamicorm.model.AttributeModel;
import com.hl.ir.utilities.dynamicorm.model.DbInteractorModel;
import com.hl.ir.utilities.dynamicorm.model.RtbObject;
import com.hl.utilities.db.DBType;

public class HlDbUtilInteractor extends DatabaseInteractor {

	public HlDbUtilInteractor(Connection connection) {
		this.connection = connection;
	}

	@Override
	public Object select(DbInteractorModel dbOperationModel) {
		try{
			Attribute[] attributes = createSelectAttributes(dbOperationModel);
			Table tables = createSelectTable(dbOperationModel);
			Table[] table;
			Map<String, Object> whereMap = (Map<String, Object>) dbOperationModel.getWhere();
			List<Table> tablesList = new ArrayList<Table>();
			Condition condition = getSelectCondition(whereMap, tablesList);
			if(whereMap.containsKey("on") && tables.getJoin() != null) {
				//add condition all keys in select json that are present in on
				Map<String, Object> onObject = (Map<String, Object>) whereMap.get("on");
				for(String onKey: onObject.keySet()){
					String[] splitKey = onKey.split("[.]");
					String[] splitValue = onObject.get(onKey).toString().split("[.]");
					condition.and(condition(WhereOperator.EQUALS, "cast( "+splitValue[0]+"."+splitValue[1]+" as text)", " cast("+splitKey[0]+".reference_object -> '"+splitValue[0]+"' as text)", true));
				}
			}
			if(!tablesList.isEmpty()) {
				table = new Table[tablesList.size()+1];
				table[0] = tables;
				int i=1;
				for(Table t:tablesList) {
					table[i] = t;
					i++;
				}
			} else {
				table = new Table[1];
				table[0] = tables;
			}

			//JSONB_AGG validation

			Where where = Where.builder().dbType(DBType.POSTGRES).set(condition).build();
			Select s = Select.builder().dbType(DBType.POSTGRES)
					.attributes(attributes) 
					.from(table) 
					.where(where)
					.build();
			return s.fire(connection).getResponse();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private Table createSelectTable(DbInteractorModel dbOperationModel) {
		Table primary = null;
		Table tempTable = null;
		Map<String, Object> select = dbOperationModel.getSelect();
		RtbObject rtbObject = dbOperationModel.getRtbObject();
		if(select.containsKey(rtbObject.getObjectId()) && rtbObject.getPrimary() != null && rtbObject.getPrimary())
			primary = table(rtbObject.getObjectId(), rtbObject.getObjectId());
		else {
			if(rtbObject.getExternalReferences() != null) {
				for(String reference: rtbObject.getExternalReferences().keySet()) {
					if(select.containsKey(reference) && rtbObject.getExternalReferences().get(reference).getPrimary() != null && rtbObject.getNonroots().get(reference).getPrimary()) {
						primary = table(reference, reference);
						break;
					}
				}
			}
			if(rtbObject.getNonroots() != null) {
				for(String nonroot: rtbObject.getNonroots().keySet()) {
					if(select.containsKey(nonroot) && rtbObject.getNonroots().get(nonroot).getPrimary() != null && rtbObject.getNonroots().get(nonroot).getPrimary()) {
						primary = table(nonroot, nonroot);
						break;
					}
				}
			}
		}
		if(primary != null) {
			for(String table: select.keySet()) {
				if(!table.equalsIgnoreCase(primary.getTableName())) {
					String on = " "+primary.getTableName()+".root_id = "+table+".root_id ";
					if(tempTable == null) 
						tempTable = primary.innerJoin(table(table, table)).on(on);
					else 
						tempTable = table(table, table).innerJoin(tempTable).on(on);
				}
			}
		}
		if(tempTable == null)
			return primary;
		return tempTable;
	}

	private Attribute[] createSelectAttributes(DbInteractorModel dbOperationModel) {
		int size = 0;
		Map<String, Object> select = dbOperationModel.getSelect();
		for(String key: select.keySet()) {
			List<String> keys = (List<String>) select.get(key);
			size += keys.size();
		}
		int i=0;
		Attribute[] attributes = new Attribute[size];
		RtbObject rtbObject = dbOperationModel.getRtbObject();
		for(String key: select.keySet()) {
			RtbObject keyObject = findKey(rtbObject, key);
			Map<String, AttributeModel> attributeModel = keyObject.getAttributes();
			List<String> keys = (List<String>) select.get(key);
			for(String attribute: keys) {
				for(String attrModelKey : attributeModel.keySet()) {
					Attribute tempAttribute = parseAttributes(attributeModel.get(attrModelKey), attrModelKey, attribute);
					if(tempAttribute != null) {
						attributes[i] = tempAttribute;
						i++;
						break;
					}
				}
			}

		}
		return attributes;
	}

	private Attribute parseAttributes(AttributeModel attributeModel, String attrModelKey, String attribute) {
		if(attributeModel.getType() != null && (attributeModel.getType().equalsIgnoreCase("boolean") || attributeModel.getType().equalsIgnoreCase("number") || attributeModel.getType().equalsIgnoreCase("string"))) {
			if(attributeModel.getColumn() != null && attributeModel.getColumn().contains(attribute)) {
				String[] split = attributeModel.getColumn().split("[.]");
				String tableAlias = split[0];
				if(attribute.contains("#")) {
					attribute = attribute.replace("#.", "").replace(tableAlias+".", "");
					return jsonb_agg(attribute(attribute, tableAlias+"$"+attrModelKey).tableAlias(tableAlias));
				}
				return attribute(attribute, tableAlias+"$"+attrModelKey).tableAlias(tableAlias);
			} 
		} else if(attributeModel.getAttributes() != null) {
			Map<String, AttributeModel> nestedAttributes = attributeModel.getAttributes();
			for(String nestedAttrModelKey: nestedAttributes.keySet()) {
				Attribute tempAttribute = parseAttributes(nestedAttributes.get(nestedAttrModelKey), nestedAttrModelKey, attribute);
				if(tempAttribute != null)
					return tempAttribute;
			}
		}
		return null;
	}

	private RtbObject findKey(RtbObject rtbObject, String key) {
		if(rtbObject.getObjectId().equalsIgnoreCase(key)) {
			return rtbObject;
		}
		else {
			if(rtbObject.getNonroots() != null) {
				for(String nonroot: rtbObject.getNonroots().keySet()) {
					if(rtbObject.getNonroots().get(nonroot).getObjectId().equalsIgnoreCase(key)) {
						return rtbObject.getNonroots().get(nonroot);
					}
				}
			}
			if(rtbObject.getExternalReferences() != null) {
				for(String reference: rtbObject.getExternalReferences().keySet()) {
					if(rtbObject.getExternalReferences().get(reference).getObjectId().equalsIgnoreCase(key)) {
						return rtbObject.getExternalReferences().get(reference);
					}
				}
			}
		}
		return null;
	}

	private Condition getSelectCondition(Map<String, Object> whereMap, List<Table> tablesList) throws Exception {
		for(String key: whereMap.keySet()) {
			Map<String, Object> keyMap = (Map<String, Object>) whereMap.get(key);
			if(!key.equalsIgnoreCase("on")) {

				if(key.equalsIgnoreCase("AND")) {
					List<Map<String, Object>> tempList = new ArrayList<Map<String,Object>>();
					for(String keyMapKey: keyMap.keySet()) {
						Map<String, Object> tempMap = new HashMap<String, Object>();
						tempMap.put(keyMapKey, (Map<String, Object>) keyMap.get(keyMapKey));
						tempList.add(tempMap);
					}
					return getSelectCondition(tempList.get(0), tablesList).and(getSelectCondition(tempList.get(1), tablesList));
				} else if(key.equalsIgnoreCase("OR")) {
					List<Map<String, Object>> tempList = new ArrayList<Map<String,Object>>();
					for(String keyMapKey: keyMap.keySet()) {
						Map<String, Object> tempMap = new HashMap<String, Object>();
						tempMap.put(keyMapKey, (Map<String, Object>) keyMap.get(keyMapKey));
						tempList.add(tempMap);
					}
					return getSelectCondition(tempList.get(0), tablesList).or(getSelectCondition(tempList.get(1), tablesList));
				} else {
					if(key.contains("#")) {
						String keyReplacement = " opt -> ";
						String[] splitHash = key.split(".#.");
						if(splitHash[1].contains(".")) {
							String[] splitDot = splitHash[1].split("[.]");
							keyReplacement += " '"+splitDot[0]+"' ";
							for(int j=1; j<splitDot.length; j++) {
								keyReplacement += " -> '"+splitDot[j]+"' ";
							}
						} else 
							keyReplacement += " '"+splitHash[1]+"' ";
						Table temp = jsonb_array_elements(splitHash[0]);
						tablesList.add(temp);
						return condition((String)keyMap.get("operator"), keyReplacement, keyMap.get("value"));
					}
					if(StringUtils.countMatches(key, ".") > 1)
						return condition((String)keyMap.get("operator"), key, keyMap.get("value"), false, true);
					return condition((String)keyMap.get("operator"), key, keyMap.get("value"));
				}
			}
		}
		return null;
	}

	@Override
	public int delete() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Object insert(DbInteractorModel dbOperationModel) {
		try{ 
			List<Insert> inserts = new ArrayList<Insert>();
			List<Integer> result = new ArrayList<Integer>();
			Gson gson = new Gson();
			List<Object> attributeNames= new ArrayList<Object>();
			List<Object> attributeValues= new ArrayList<Object>();
			Map<String, Object> insertJson = dbOperationModel.getInsert();
			RtbObject rtbObject = dbOperationModel.getRtbObject();
			for(String key : insertJson.keySet()) {
				RtbObject insertObject = findKey(rtbObject, key);
				if(!insertJson.containsKey("id")) {
					//use sequence for id
					if(insertObject.getRoot() != null && insertObject.getRoot()) {
						if(insertJson.containsKey("root_id")) {
							//use the same number generated by above sequence for root_id
						}
					}
				}
				if(insertObject.getPrimary() == null || !insertObject.getPrimary()) {
					System.out.println("throw Exception Cannot insert non primary object");
				} else {
					Map<String, Object> insertData = (Map<String, Object>) insertJson.get(key);
					for(String insertKey: insertData.keySet()) {
						Map<String, AttributeModel> attributes = insertObject.getAttributes();
						if(attributes.containsKey(insertKey)) {
							String column = attributes.get(insertKey).getColumn();
							column = column.substring(column.indexOf(key+".")+key.length()+1);
							attributeNames.add(column);
							String type = attributes.get(insertKey).getType();
							if((insertData.get(insertKey) instanceof Map || insertData.get(insertKey) instanceof List) && (type.equalsIgnoreCase("object") || type.equalsIgnoreCase("array"))) {
								validateJsonbStructure(insertData.get(insertKey), insertKey, insertObject);
								attributeValues.add(gson.toJson(insertData.get(insertKey)));
							} else {
								attributeValues.add(insertData.get(insertKey));
							}
						}
					}
					Insert insert= Insert.builder().dbType(DBType.POSTGRES)
							.into(key)
							.attributeNames(attributeNames)
							.attributeValues(attributeValues)
							.build();
					inserts.add(insert);
				}
			}
			for(Insert insert: inserts)
				result.add((Integer) insert.fire().getResponse());
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private void validateJsonbStructure(Object data, String insertKey, RtbObject insertObject) {
		if(data instanceof List) {
			//loop
		} else if (data instanceof Map) {
			//no loop
		}
	}

	@Override
	public int update(DbInteractorModel dbOperationModel) {
		JsonObject request= (JsonObject) (dbOperationModel.getUpdate().get("request"));
		Set<String> objectNameSet= request.keySet();
		List<Update> updateList= new ArrayList<> ();
		for (String objectId : objectNameSet) {
			JsonElement updateJson= request.get(objectId);
			RtbObject object= dbOperationModel.getRtbObject();
			//the line below fetches the intended object from the root object and sets the attributes map accordingly
			Map<String, AttributeModel> attributes= object.getObjectId().equals(objectId) ? object.getAttributes() : (object= object.getNonroots().get(objectId)).getAttributes();
			String ouid= object.getOuid();
			try {
				if (object.getOutput()) {
					if (updateJson.isJsonObject()) {
						UpdateBuilder update= this.prepareForUpdate(objectId, ouid, attributes,updateJson.getAsJsonObject(), null, null);
						Where where=  Where.builder()
								.dbType(DBType.POSTGRES)
								.set(condition("=", ouid, updateJson.getAsJsonObject().get(ouid)).and(condition("=", object.getRootId(),updateJson.getAsJsonObject().get(object.getRootId()))))
								.build();
						update.where(where);
						updateList.add(update.build());
						
					} else if (updateJson.isJsonArray()) {
						JsonArray array= updateJson.getAsJsonArray();
						for (int i= 0; i < array.size();i++) {
							UpdateBuilder update= this.prepareForUpdate(objectId, ouid, attributes,array.get(i).getAsJsonObject(), null, null);
							Where where=  Where.builder()
									.dbType(DBType.POSTGRES)
									.set(condition("=", ouid, array.get(i).getAsJsonObject().get(ouid)).and(condition("=", object.getRootId(), array.get(i).getAsJsonObject().get(object.getRootId()))))
									.build();
							update.where(where);
							updateList.add(update.build());
						}
					}

				}
			} catch (Exception e) {
				e.printStackTrace();
				return -1;
			}
		}
		
		byte counter= this.fireUpdate(updateList);
		return counter;
	}
	
	private byte fireUpdate(List<Update> updateList) {
		byte counter= 0;
		boolean isConnectionNull= false;
		try {
			
			if (connection == null) {
				isConnectionNull= true;
			} else {
				connection.setAutoCommit(false);
			}
			for (Update update : updateList) {
//				System.out.println(update.toString());
				//also get to know the response of this service and decide whether the update was a success or not
				if (isConnectionNull)
					update.fire();
				else
					update.fire(connection);
				counter++;
			}
			if (!isConnectionNull)
				connection.commit();
		} catch (QueryException | SQLException e) {
			e.printStackTrace();
			try {
				if (!isConnectionNull)
					connection.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			counter= -1;
		}
		return counter;
	}

	private UpdateBuilder prepareForUpdate(String objectId, String ouid, Map<String, AttributeModel> attributes, JsonObject updateJson, UpdateBuilder update, JsonObject uniqueCriteriaForAspectArrays) {
		Set<String> updateKeys= updateJson.keySet();
		for (String key : updateKeys) {
			AttributeModel attribute= attributes.get(ouid.equals(key)?"id":key);

			if (!("object".equalsIgnoreCase(attribute.getType()) || "array".equalsIgnoreCase(attribute.getType()))) {
				String column= attribute.getColumn();
				String tableName= column.substring(0, column.indexOf("."));

				if (update == null) {
					update= Update.builder().dbType(DBType.POSTGRES).table(tableName);
				}

				try {
					if (uniqueCriteriaForAspectArrays == null) {
						update.set(column.substring(column.indexOf(".") + 1, column.length()), this.fetchValue(key, attribute.getType(), updateJson));
					} else {
						update.set(column.substring(column.indexOf(".") + 1, column.length()), this.fetchValue(key, attribute.getType(), updateJson), uniqueCriteriaForAspectArrays);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				continue;
			} else {
				if ("object".equalsIgnoreCase(attribute.getType())) {
					update= this.prepareForUpdate(objectId, ouid, attributes.get(key).getAttributes(),updateJson.get(key).getAsJsonObject(), update, uniqueCriteriaForAspectArrays);
					continue;
				}

				if ("array".equalsIgnoreCase(attribute.getType())) {
					//set column value with unique key, need to loop through the json array items and prepareForUpdate. this time need to pass the unique key json along for every item in the array
					//need to create the unique key json and pass it here.
					try {
						JsonArray array= updateJson.get(key).getAsJsonArray();
						for (JsonElement element : array) {
							//iterate through the object's key set, all keys except the core_attributes is the unique id
							JsonObject object= element.getAsJsonObject();
							Map<String, AttributeModel> attributesMap= attributes.get(key).getAttributes();
							Set<String> keys= attributesMap.keySet();
							//keys.remove("core_attributes");
							JsonObject uniqueMap= new JsonObject();
							for (String uniqueId : keys) {
								if ("core_attributes".equals(uniqueId))
									continue;
								String type= attributesMap.get(uniqueId).getType();
								switch (type) {
								case "number":
									uniqueMap.addProperty(uniqueId, object.get(uniqueId).getAsNumber());
									break;
								case "string":
									uniqueMap.addProperty(uniqueId, object.get(uniqueId).getAsString());
									break;
								case "boolean":
									uniqueMap.addProperty(uniqueId, object.get(uniqueId).getAsBoolean());
									break;
									default:
										uniqueMap.addProperty(uniqueId, object.get(uniqueId).getAsString());
										
								}
							}
							update= this.prepareForUpdate(objectId, ouid, attributesMap,object.getAsJsonObject(), update, uniqueMap);
						}
						//						update.set(attribute.getColumn(), this.fetchValue(key, attribute.getType(), updateJson));
					} catch (Exception e) {
						e.printStackTrace();
					}
					continue;
				}

			}
		}


		return update;
	}

	private Object fetchValue(String key, String type, JsonObject updateJson) {
		//		String[] keys= level.append(key).toString().split("[.]");
		//		level.append(".");
		JsonObject temp= updateJson;
		//		for (int i= 0; i < keys.length; i++) {
		//			JsonElement element= temp.get(keys[i]);
		JsonElement element= temp.get(key);
		if (!(element.isJsonObject() || element.isJsonArray())) {
			switch (type) {
			case "number":
				return element.getAsNumber();
			case "string":
				return element.getAsString();
			case "boolean":
				return element.getAsBoolean();
			}
		}
		//			temp= temp.get(keys[i]).getAsJsonObject();
		//		}
		return null;
	}


}
