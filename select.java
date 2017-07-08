package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;


public class select {
	
	public static StringBuilder mulSelectRow(HashMap<String, HashMap<String, String>> schema,
			StringBuilder row, ArrayList<String> selectQuery,ArrayList<String> tableNames){
		/*Ex.
		 * selectQuery.get(0):P.p_brand = 'Brand#12' 
		 * AND P.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		 * selectQuery.get(1):P.p_brand = 'Brand#23'
		 * AND P.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		 * selectQuery.get(2): P.p_brand = 'Brand#34'
		 * AND P.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		*/
		if(row.length()>0){
			String query;
			String[] Queries;
			StringBuilder recordRow = new StringBuilder();
			recordRow.append(row);
		
			for (int i = 0; i < selectQuery.size(); i++) {
				Queries = selectQuery.get(i).split("\\|");
				
				for (int j = 0; j < Queries.length; j++) {
					
					query = Queries[j];
					if(query.contains("in")&&!query.contains("=")&&!query.contains("like")){
						inSelect(schema, row, query, tableNames);
					}else{
						selectRow(schema, row, query, tableNames);
					}
					if(row.length() == 0){
						break;
					}
				}
				if(row.length() == 0){
					if(i != selectQuery.size()-1){
						row.append(recordRow);
					}
				}else{
					return row;
				}
			}
		}
		
		return row;
	}
	
	//query contains upper cases and lower cases
	public static StringBuilder selectRow(HashMap<String, HashMap<String, String>> schema,
			StringBuilder row, String query,ArrayList<String> tableNames){
		if(schema.size()>0 && row.length()>0){
			//Ex. query = "publication.pubDate > '2017-01-01'";
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
	
	//"in" operator
	public static StringBuilder inSelect(HashMap<String, HashMap<String, String>> schema,
			StringBuilder row, String query, ArrayList<String> tableNames){
		//P.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		//parse query into two parts
		String[] qstr = query.split("\\(");
		String inQuery = qstr[0].substring(0, qstr[0].length()-1);
		//get condition
		String[] operands = qstr[1].split("\\'");
		ArrayList<StringBuilder> conditions = new ArrayList<>();
		for (int i = 0; i < operands.length; i++) {
			if(!operands[i].contains("'")||!operands[i].contains(")")){
				StringBuilder sb = new StringBuilder(operands[i]);
				conditions.add(sb);
			}
		}		
		InSelect(schema, row, inQuery, conditions, tableNames);
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
