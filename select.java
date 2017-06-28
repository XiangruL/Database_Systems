package sql.evaluator;

import java.util.HashMap;

import ParseSqlQuery.tool;

public class select {

	public StringBuilder selectRow(HashMap<String, HashMap<String, String>> scheme,
			StringBuilder row, String query){
		StringBuilder sb = new StringBuilder();
		//Ex. query = "pubDate > '2017-01-01'";
		//get table name
		String[] rowStrings = row.toString().split("\\|");
		String tableName = rowStrings[rowStrings.length-1];
		//get the type and column of attribute
		String[] str = query.trim().split("\\s+");
		String colName = str[0];
		
		int colNum = tool.getColNum(scheme, tableName, colName);
		String type = tool.getColType(scheme, tableName, colName);
		//get the operator from query
		String operator = str[1];
				
		//cast the type of constant based on the variable type
		String operand = str[2];
		
		String temp;
		if(type.contains("varchar") || type.equals("date")){
			temp = operand.substring(1, operand.length()-1);
			operand = temp;
		}
		
		if(operator.equals("like")){
			operator = "=";
		}
		
		temp = rowStrings[colNum];
	
		switch (operator) {
		case "=":
			if(temp.equals(operand)){
				sb.append(row);
				return sb;
			}
			break;
			
		case ">":
			if(tool.isLarge(type, temp, operand)){
				sb.append(row);
				return sb;
			}
			
			break;
			
		case "<":
			if(tool.isLarge(type,operand,temp)){
				sb.append(row);
				return sb;
			}
			

		default:
			break;
		}	
		
		
		
		return null;
	}
	
}
