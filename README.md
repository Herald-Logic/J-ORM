# J-ORM (Json - Object Relation Mapping)

JSON Schema
===========

Author
------

Nikhil Latkar (nikhil.latkar@wonderlendhubs.com)
Mohammed Zain Memon (mohammed.zain@wonderlendhubs.com)
Pranav Srinivas Dutta Sriraj (pranav.dutta@heraldlogic.com)
Mihir Londhe (mihir.londhe@heraldlogic.com)
Rupesh Sharma (rupesh.sharma@wonderlendhubs.com)

Modification
-------------

* Insertion / Updation of JSONB attributes to the RDBMS table is supported from this version.
* This version of the Dynamic ORM is absolutely backwards compatible.

Inspiration
-----------
https://json-schema.org/specification.html

Introduction
-------------

The idea of JSON Schema primarily is to enable various operations by just parsing a JSON Request. This enables users to additionally add or remove fields or also modify fields and their behaviour by just changing a JSON config.

Features
---------

- Inline Validations  (By 3rd Party (https://json-schema.org))
- Custom Validations
- Post Processor
- Generating a JSON (usually response) out of values from database
- Inserting/Updating values in database while parsing a JSON (usually request)

Advantages
----------
- Ease of use
- Highly configurable
- No compilation of code on any change
- Almost Zero Goto Market Time (Depends on use case)
- Zero Downtime Deployment

Keywords
---------

- "type" : String value of this keyword defines the datatype of the instance.
	- Value
		- "boolean"	: A "true" or "false" value.
		- "object"	: An unordered set of properties mapping a string to an instance.
		- "array"	: An ordered list of instances.
		- "array-object" : An ordered list of instances having unique keys for each.
		- "number" 	: A decimal number value.
		- "string" 	: A text value.

- "columns" (Deprecated (to be renamed)): Array of String which exactly points the source/destination property from source structure where the value will be fetched/stored.
	- Each value of this keyword has to be fully qualified in the following syntax (<objectname>.<propertyname>).
		- eg. applicant.first_name (where applicant = object name and first_name = propertyname)

- "properties" : List of unordered properties mapping when "type" == "object"
	- This keyword is mandatory if "type" is "object"

- "items" : Definition of each of the item when "type" == "array" or "type" == "array-object"
	- This keyword is mandatory if "type" is "array"/"array-object"

- "default" : Provides value to the instance when instance == null
	- Either a value directly can be provided or derived by calling a method.
	- syntax : "default":{
			        "type" : "value|method",
			        "value" : "<value|fully qualified method name>",
			        "params" : <params for the method to be called in the form of a json>
			    }

- "validator" : Custom validation on an instance
	- A method will be called in which validation can be implemented.
	- syntax : "validator":{
			        "value" : "<fully qualified method name>",
			        "params" : <params for the method to be called in the form of a json>
			        //value to be validated will be passed to method in a hashMap with key "value" along with other params mentioned
			    }

- "processor" : Works as a post processor. If a value is returned from processor, the value is updated to the corresponding destination property (simulating pre-processor).
    - A method will be called after all instances are processed.
    -  syntax : "processor":{
			        "value":"<fully qualified method name>",
			        "params":<params for the method to be called in the form of a jsonObject>
			        //params will be passed to the method in a hashmap along with the value of <jsonKey> as "value"
			    }

- "referenceKey" : Cache the value of the instance with the value of the keyword and make it available to the current and child objects.
	- Reference Keys values are by default added in the where clause if referred to in the children block.

- "internalColumns" (Deprecated) : Used to hide the instance from the Request/Response
	- Example : Want to use a property in where clause of an object, but should be hidden to the requestor or fetcher.

- "id" : A mandatory property under every "type" == "object" instance. Uniquely identifies the object instance in source/destination.

- "sort" : Orders the items in specified order when "type" == "array"/"array-object"

- "objectKey" : Denotes the property where value of keys of each item is to be fetched when "type" == "array-object" and corresponding items has "type" == "object"
	- Example : "objectKey" = "applicantId" will make resultant json having keys as values of applicantId.
	- Avoids iteration for the consumer of the JSON.
	- To be used only while generating a JSON.
	- 
- "jsonb" : This is a keyword that is used to identify an attribute (with "type" as "object") that needs to be persisted in a jsonb column in database.
