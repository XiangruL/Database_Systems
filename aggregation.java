package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;

public class aggregation {
	
	public ArrayList<StringBuilder> aggRow(HashMap<String, HashMap<String, String>> schema,
			StringBuilder row1, ArrayList<StringBuilder> result, String query){
		
		//Ex. query = "count(author.instituteid)" 
		//Ex. query = "count(instituteid)";
		
		String[] qstr = query.split("\\(");
		String operator = qstr[0];
		String tcName = qstr[1].substring(0,qstr[1].length()-1);
		String tableName = "";
		String colName = "";
		if(tcName.contains(".")){
			String[] tcNames = tcName.split("\\.");
			tableName = tcNames[0];
			colName = tcNames[1];
		}else{
			ArrayList<String> tName = new ArrayList<>();
			tName= parseSelect.getFromTable();
			tableName = tName.get(0);
			colName = tcName;
		}
		
		switch (operator) {
		case "count":
			
			break;
		
		case "sum":
			
			break;
			
		case "avg":
	
			break;
	
		case "min":
	
			break;
	
		case "max":
	
			break;

		default:
			break;
		}
		
		
		
		
		
		return result;
	}

}
