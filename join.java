package sql.evaluator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class join {
	HashMap<String, HashMap<String, String>> newSchema = new HashMap<>();
	
	public StringBuilder joinRow(HashMap<String, HashMap<String, String>> schema,
			StringBuilder row1, StringBuilder row2, String tableName1, String tableName2,
			String colName1, String colName2, boolean flag){
		StringBuilder row = new StringBuilder();
		row.append(row1.toString());
		String[] rowString1 = row.toString().split("\\|");
		String[] rowString2 = row2.toString().split("\\|");
		
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
		
		
		
		return row;
	}
	
	public void createSchema(HashMap<String, HashMap<String, String>> schema,
			String tableName1, String tableName2, String columnName){
		
		HashMap<String, String> colHashMap1 = new HashMap<>();
		HashMap<String, String> colHashMap2 = new HashMap<>();
		
		//find the total number of columns of first table
		int count = schema.get(tableName1).size();
		
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
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        if(columnName.equals((String)pair.getKey())){
	        	continue;
	        }
	        temp = (String) pair.getValue();
	        str = temp.split(",");
	        str[0] = String.valueOf(count);
	        temp = str[0]+","+str[1];
	        count++;
	        colHashMap2.put((String)pair.getKey(), temp);
	        
	    }
	    newSchema.put(tableName2, colHashMap2);
		
	}

	public HashMap<String, HashMap<String, String>> getNewSchema() {
		return newSchema;
	}

	
	
	
}
