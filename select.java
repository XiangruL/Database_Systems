package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;


public class select {
	
	//query contains upper cases and lower cases
	public static StringBuilder selectRow(HashMap<String, HashMap<String, String>> schema,
			StringBuilder row, String query,ArrayList<String> tableNames){
		
		//StringBuilder sb = new StringBuilder();

		if(schema.size()>0 && row.length()>0){
			//Ex. query = "publication.pubDate > '2017-01-01'";
			
//			if(query.contains("in")){
//				InSelect(schema, row, query);
//				return row;
//			}
			
			
			//get table name
			//get the type and column of attribute
			String[] qstr = query.split("\\s+");
			
			String tableName = null;
			String colName = null;
			
			//check if contains table name
			if(qstr[0].contains(".")){
				String[] tcName = qstr[0].toLowerCase().split("\\.");
				tableName = tcName[0];
				colName = tcName[1];
			}else{
				tableName = tableNames.get(0);
				colName = qstr[0].toLowerCase();
				
			}
			
			
			int colNum = tool.getColNum(schema, tableName, colName);
			String type = tool.getColType(schema, tableName, colName);
			//get the operator from query
			String operator = qstr[1];
			
			//combine the opreand which might be splited by space
			String operand = qstr[2];
			for (int i = 3; i < qstr.length; i++) {
				operand +=" "+qstr[i];
			}
			
			
			//cast the type of constant based on the variable type
			String temp;
			
			if(operand.contains("'")){
				temp = operand.substring(1, operand.length()-1);
				operand = temp;
			}
			
			if(operator.equals("like")){
				operator = "=";
			}
			
			if(operator.equals("notlike")){
				operator = "!=";
			}
			
			String[] rowStrings = row.toString().split("\\|");
			temp = rowStrings[colNum];
			switch (operator) {
			
			case "!=":
				
				if(!temp.equals(operand)){
					return row;
				}
				break;
			
			case "=":
				
				if(temp.equals(operand)){
					return row;
				}
				break;
				
			case ">":
		
				if(tool.isLarge(type, temp, operand)){
					return row;
				}
				
				break;
				
			case "<=":
				
				if(!tool.isLarge(type, temp, operand)){
					return row;
				}
				
				break;
				
			case "<":
				if(tool.isSmall(type, temp, operand)){
					return row;
				}
				break;
				
			case ">=":
				
				if(!tool.isSmall(type, temp, operand)){
					return row;
				}
				break;
				

			default:
				break;
			}	
			row.delete(0, row.length());
		}
		return row;
	}
	
	//"IN" operator 
	public static StringBuilder InSelect(HashMap<String, HashMap<String, String>> schema,
			StringBuilder row, String query, ArrayList<StringBuilder> result,
			ArrayList<String> tableNames){
		//Ex. query = "";
		//get table name
		//get the column of attribute
		if(row.length() > 0){
			String[] qstr = query.split("\\s+");
			
			String tableName = null;
			String colName = null;
			
			//check if contains table name
			if(qstr[0].contains(".")){
				String[] tcName = qstr[0].toLowerCase().split("\\.");
				tableName = tcName[0];
				colName = tcName[1];
			}else{
				tableName = tableNames.get(0);
				colName = qstr[0].toLowerCase();
				
			}
			
			
			int colNum = tool.getColNum(schema, tableName, colName);
			String rowString = row.toString().split("\\|")[colNum];
			
			for (int i = 0; i < result.size(); i++) {
				if(rowString.equals(result.get(i).toString())){
					return row;
				}
			}
			row.delete(0, row.length());
		}
		
		
		return row;
	}
	
}
