package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;

public class projection {
	
	
	//if flag is true, do distinct first, then do projection and store it in the array list
	//projection line by line
	public static ArrayList<StringBuilder> proTable(ArrayList<StringBuilder> result,
			HashMap<String, HashMap<String, String>> schema, 
			StringBuilder row,String query,boolean flag){
		
		if(flag){
			
			distinct.dist(result, schema, row, query);
		}
		
		if(row.length()>0){
			//Parse the query, then get table name and column name
			String[] qstr = query.split(", ");
			
			String[] rowString = row.toString().split("\\|");
			String tableName = "";
			String colName = "";
			int colNum = 0;
			StringBuilder sb = new StringBuilder();
			
			for (int i = 0; i < qstr.length; i++) {
				if(qstr[i].contains(".")){
					String[] tcName = qstr[i].split("\\.");
					tableName = tcName[0];
					colName = tcName[1];
				}else{
					ArrayList<String> tName = parseSelect.getFromTable();
					if(tName.size()!=1){
						System.err.println("do not specify table name");
						return result;
					}
					tableName = tName.get(0);
					colName = qstr[i];
				}
				
				colNum = tool.getColNum(schema, tableName, colName);
				
				if(i == qstr.length-1){
					sb.append(rowString[colNum]);
					break;
				}
				sb.append(rowString[colNum]+",");
			}
			
			result.add(sb);			
		}
		return result;
	}
	
	
	//projection a result table
	//if flag is true, do the distinct first
	public static ArrayList<StringBuilder> proTable(ArrayList<StringBuilder> result,
			HashMap<String, HashMap<String, String>> schema, String query, boolean flag){
		
		if(flag){
			distinct.dist(result, schema, query);
		}
		
		StringBuilder sb = new StringBuilder();
		String[] qstr = query.split(", ");
		String tableName;
		String colName;
		int colNum;
		String[] rowStrings;
		for (int i = 0; i < result.size(); i++) {
			rowStrings = result.get(i).toString().split("\\|");
			
			for (int j = 0; j < qstr.length; j++) {
				if(qstr[j].contains(".")){
					String[] tcName = qstr[j].split("\\.");
					tableName = tcName[0];
					colName = tcName[1];
				}else{
					ArrayList<String> tName = parseSelect.getFromTable();
					if(tName.size()!=1){
						System.err.println("do not specify table name");
						return result;
					}
					tableName = tName.get(0);
					colName = qstr[j];
				}
				
				
				colNum = tool.getColNum(schema, tableName, colName);
				
				if(j == qstr.length-1){
					sb.append(rowStrings[colNum]);
					break;
				}
				sb.append(rowStrings[colNum]+",");
				
			}	
			result.remove(i);
			result.add(i,sb);	
			sb = new StringBuilder();
		}
		return result;
	}
}
