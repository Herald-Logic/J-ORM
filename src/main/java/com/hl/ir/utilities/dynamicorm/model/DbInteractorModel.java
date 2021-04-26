package com.hl.ir.utilities.dynamicorm.model;

import java.util.Map;

public class DbInteractorModel {

	RtbObject rtbObject;
	Map<String, Object> where;
	Map<String, Object> values;
	Map<String, Object> select;
	Map<String, Object> insert;
	Map<String, Object> update;

	public RtbObject getRtbObject() {
		return rtbObject;
	}
	public void setRtbObject(RtbObject rtbObject) {
		this.rtbObject = rtbObject;
	}
	public Map<String, Object> getWhere() {
		return where;
	}
	public void setWhere(Map<String, Object> where) {
		this.where = where;
	}
	public Map<String, Object> getValues() {
		return values;
	}
	public void setValues(Map<String, Object> values) {
		this.values = values;
	}
	public Map<String, Object> getSelect() {
		return select;
	}
	public void setSelect(Map<String, Object> select) {
		this.select = select;
	}
	public Map<String, Object> getInsert() {
		return insert;
	}
	public void setInsert(Map<String, Object> insert) {
		this.insert = insert;
	}
	public void setUpdate(Map<String, Object> update) {
		this.update= update;
	}
	public Map<String, Object> getUpdate() {
		return update;
	}
	
}
