package com.hl.ir.utilities.dynamicorm.model;

import java.util.List;
import java.util.Map;

public class RtbObject {

	private String objectId;
	private String ouid;
	private Boolean root;
	private Boolean input;
	private Boolean output;
	private Boolean primary;
	private Boolean multiple;
	private final String rouid = "root_id";
	private Map<String, AttributeModel> attributes;  //core_attributes : ["a","b.c","b.d","b.e.f","b.e.h","g"], loanapploanoffers : ["a","b","c","d"]
	private Map<String, List<String>> uniqueKeys;
	private Map<String, RtbObject> nonroots; //primary, rtbobject if non root is primary
	private Map<String, AttributeModel> internalReferences;
	private Map<String, RtbObject> externalReferences;

	public Boolean getRoot() {
		return root;
	}
	public void setRoot(Boolean root) {
		this.root = root;
	}
	public Boolean getInput() {
		return input;
	}
	public void setInput(Boolean input) {
		this.input = input;
	}
	public Boolean getOutput() {
		return output;
	}
	public void setOutput(Boolean output) {
		this.output = output;
	}
	public Boolean getPrimary() {
		return primary;
	}
	public void setPrimary(Boolean primary) {
		this.primary = primary;
	}
	public String getOuid() {
		return ouid;
	}
	public void setOuid(String ouid) {
		this.ouid = ouid;
	}
	public String getRootId() {
		return rouid;
	}
	public String getObjectId() {
		return objectId;
	}
	public void setObjectId(String objectId) {
		this.objectId = objectId;
	}
	public Map<String, List<String>> getUniqueKeys() {
		return uniqueKeys;
	}
	public void setUniqueKeys(Map<String, List<String>> uniqueKeys) {
		this.uniqueKeys = uniqueKeys;
	}
	public Map<String, RtbObject> getExternalReferences() {
		return externalReferences;
	}
	public void setExternalReferences(Map<String, RtbObject> externalReferences) {
		this.externalReferences = externalReferences;
	}
	public Map<String, RtbObject> getNonroots() {
		return nonroots;
	}
	public void setNonroots(Map<String, RtbObject> nonroots) {
		this.nonroots = nonroots;
	}
	public Boolean getMultiple() {
		return multiple;
	}
	public void setMultiple(Boolean multiple) {
		this.multiple = multiple;
	}
	public Map<String, AttributeModel> getAttributes() {
		return attributes;
	}
	public void setAttributes(Map<String, AttributeModel> attributes) {
		this.attributes = attributes;
	}
	public Map<String, AttributeModel> getInternalReferences() {
		return internalReferences;
	}
	public void setInternalReferences(Map<String, AttributeModel> internalReferences) {
		this.internalReferences = internalReferences;
	}
}
