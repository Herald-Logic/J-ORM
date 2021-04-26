import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.hl.ir.utilities.dynamicorm.ORMSchema;
import com.hl.ir.utilities.dynamicorm.model.JsonSchemaModel;
import com.hl.ir.utilities.properties.Client;
import com.hl.ir.utilities.properties.exception.PropertyNotFoundException;

public class Main {
	public static void main(String[] args) throws PropertyNotFoundException {
		Client.builder().domain("wlh").client("demo").service("ir_lookup_cud").build();
		
		Map<String, JsonSchemaModel> schema = new Gson().fromJson("{\"loanapp\":{\"root\":true,\"input\":true,\"output\":true,\"primary\":true,\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"number\",\"columns\":[\"loanapp.loan_app_id\"],\"referenceId\":\"loan_app_id\"},\"root_id\":{\"clause\":false,\"type\":\"number\",\"columns\":[\"loanapp.root_id\"],\"referenceId\":\"root_id\"},\"loanapploanoffers\":{\"type\":\"array\",\"jsonb\":true,\"items\":{\"type\":\"object\",\"properties\":{}}}},\"references\":{\"product\":{\"type\":\"object\",\"reference\":true,\"input\":true,\"properties\":{\"id\":{\"type\":\"number\",\"columns\":[\"wlhproduct.product\"],\"referenceId\":\"product\"}}},\"hub_entity\":{\"type\":\"object\",\"reference\":true,\"input\":true,\"properties\":{\"id\":{\"type\":\"number\",\"columns\":[\"entity.entity_code\"],\"referenceId\":\"entity_code\"}}}}},\"applicant\":{\"root\":false,\"input\":true,\"output\":false,\"primary\":false,\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"applicant_id\":{\"type\":\"number\",\"columns\":[\"applicant.applicant_id\"],\"referenceId\":\"applicant_id\"},\"root_id\":{\"clause\":true,\"type\":\"number\",\"columns\":[\"applicant.root_id\"],\"referenceId\":\"root_id\"}}}},\"applicantaggregate\":{\"root\":false,\"input\":true,\"output\":false,\"primary\":false,\"items\":{\"type\":\"object\",\"properties\":{\"unique_id\":{\"type\":\"number\",\"columns\":[\"applicantaggregate.unique_id\"],\"referenceId\":\"unique_id\"},\"root_id\":{\"clause\":true,\"type\":\"number\",\"columns\":[\"applicantaggregate.root_id\"],\"referenceId\":\"root_id\"},\"applicant_id\":{\"clause\":true,\"jsonb\":true,\"type\":\"number\",\"key\":[\"applicant\"],\"on\":\"applicant.properties.applicant_id\",\"columns\":[\"applicantaggregate.referenes\"],\"referenceId\":\"applicantaggregate.applicant_id\"}}}}}", new TypeToken<Map <String, JsonSchemaModel>>(){}.getType());
		ORMSchema ormschema = ORMSchema.builder().schema(schema).build();
		System.out.println(ormschema.validate(new Gson().fromJson("{\"book\":{\"id\":{}}}", JsonObject.class)));
//		System.out.println(ormschema.fetch(new Gson().fromJson("{\"and\":{\"book_id\":{\"operator\":\"=\",\"value\":2},\"author_id\":{\"operator\":\"=\",\"value\":1}}}", JsonObject.class)));
		System.out.println(
				ormschema.update(new Gson().fromJson("{\"and\":{\"book_id\":{\"operator\":\"=\",\"value\":2},\"author_id\":{\"operator\":\"=\",\"value\":1}}}", JsonObject.class)));
	}
}
