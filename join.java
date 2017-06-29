package sql.evaluator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class join {
	static HashMap<String, HashMap<String, String>> newSchema = new HashMap<>();
	
	public static StringBuilder joinRow(HashMap<String, HashMap<String, String>> schema,
			StringBuilder row1, StringBuilder row2, String query, boolean flag){
		
		StringBuilder row = new StringBuilder();
		if(row1.length()>0){
			
			row.append(row1.toString());
			String[] rowString1 = row.toString().split("\\|");
			String[] rowString2 = row2.toString().split("\\|");
			
			//parse query to get table name and column name
			String[] qstr = query.split("\\s+");
			String[] tcName1 = qstr[0].split("\\.");
			String[] tcName2 = qstr[2].split("\\.");
			
			String tableName1 = tcName1[0];
			String tableName2 = tcName2[0];
			
			String colName1 = tcName1[1];
			String colName2 = tcName2[1];
			
			int colNum1 = tool.getColNum(schema, tableName1, colName1);
			int colNum2 = tool.getColNum(schema, tableName2, colName2);
			
			//flag is used to check if this is the first time join two tables
			if(!flag){
				//create schema
			
				createSchema(schema,tableName1,tableName2,colName2);
			}
			
			if(rowString1[colNum1].equals(rowString2[colNum2])){
				
				for (int i = 0; i < rowString2.length; i++) {
					if(!rowString2[i].equals(colName2)){
						if(i == rowString2.length-1){
							row.append(rowString2[i]);
							break;
						}
						row.append(rowString2[i]+"|");
					}
				}
				
			}else{
				row.delete(0, row.length());
			}
		}
		
		
		
		
		return row;
	}
	
	public static void createSchema(HashMap<String, HashMap<String, String>> schema,
			String tableName1, String tableName2, String columnName){
		
		HashMap<String, String> colHashMap1 = new HashMap<>();
		HashMap<String, String> colHashMap2 = new HashMap<>();
		
		//find the total number of columns of first table
		int count = schema.get(tableName1).size()-1;
		
		//assign index for the second table column
		String temp = "";
		String[] str;
		
		Iterator it = schema.get(tableName1).entrySet().iterator();
		while (it.hasNext()) {
		       Map.Entry pair = (Map.Entry)it.next();
		       colHashMap1.put((String)pair.getKey(), (String)pair.getValue());
		      
		}
		newSchema.put(tableName1, colHashMap1);
		
		it = schema.get(tableName2).entrySet().iterator();
		String colName;
		int colNum;
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        if(columnName.equals((String)pair.getKey())){
	        	colNum = tool.getColNum(schema, tableName1, columnName);
	        	temp = (String) pair.getValue();
		        str = temp.split(",");
		        str[0] = String.valueOf(colNum);
		        temp = str[0]+","+str[1];
	        	colHashMap2.put(columnName, temp);
	        	continue;
	        }
	        colName = (String)pair.getKey();
	        colNum = tool.getColNum(schema, tableName2, colName)+count;
	        temp = (String) pair.getValue();
	        str = temp.split(",");
	        str[0] = String.valueOf(colNum);
	        temp = str[0]+","+str[1];
	        
	        colHashMap2.put(colName, temp);
	        
	    }
	    newSchema.put(tableName2, colHashMap2);
		
	}

	public static HashMap<String, HashMap<String, String>> getNewSchema() {
		return newSchema;
	}

}
