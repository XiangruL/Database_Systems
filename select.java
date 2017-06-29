package sql.evaluator;

import java.util.HashMap;


public class select {
	//query contains upper cases and lower cases
	public StringBuilder selectRow(HashMap<String, HashMap<String, String>> schema,
			StringBuilder row, String query){
		StringBuilder sb = new StringBuilder();
		//Ex. query = "publication.pubDate > '2017-01-01'";
		//get table name
		String[] rowStrings = row.toString().split("\\|");

		//get the type and column of attribute
		String[] qstr = query.trim().split("\\s+");
		String[] tcName = qstr[0].toLowerCase().split("\\.");
		String tableName = tcName[0];
		String colName = tcName[1];
		
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

		return sb;
	}
	
}
