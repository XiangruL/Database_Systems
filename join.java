package sql.evaluator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class join {
	static HashMap<String, HashMap<String, String>> newSchema = null;
	static int index = 0;
	
	public static StringBuilder joinRow(HashMap<String, HashMap<String, String>> schema,HashMap<String, HashMap<String, String>> oldschema,
			StringBuilder row1, StringBuilder row2, String query, boolean flag, boolean updatFlag){
		
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
			int colNum2 = tool.getColNum(oldschema, tableName2, colName2);

					
			if(rowString1[colNum1].equals(rowString2[colNum2])){
			
				for (int i = 0; i < rowString2.length; i++) {
					if(i!=colNum2){
						if(i == rowString2.length-1){
							row.append("|"+rowString2[i]);
							break;
						}
						
						row.append("|"+rowString2[i]);
					}
				}
				
			}else{
				row.delete(0, row.length());
			}
			
			//flag is used to check if this is the first time join two tables
			if(updatFlag){
				if(flag){
					//create schema
					createSchema(schema, tableName1, tableName2, colName1, colName2);
				}else{
					createSchema1(schema, tableName1, tableName2, colName1, colName2);
				}
			}
			
			
		}
			
		return row;
	}
	
	//update one table schema
	public static void createSchema1(HashMap<String, HashMap<String, String>> schema,
			String tableName1,String tableName2, String colName1, String colName2){
		 
		newSchema = new HashMap<>(newSchema);

		HashMap<String, String> colHashMap1 = new HashMap<>();
		HashMap<String, String> colHashMap2 = new HashMap<>();
		
		//assign index for the second table column
		String temp = "";
		String[] str;
		
		Iterator it = schema.get(tableName2).entrySet().iterator();
		String colName;
		int colNum;
				
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        if(colName2.equals((String)pair.getKey())){
	        	colNum = tool.getColNum(schema, tableName1, colName1);
	        	temp = (String) pair.getValue();
		        str = temp.split(",");
		        str[0] = String.valueOf(colNum);
		        temp = str[0]+","+str[1];
	        	colHashMap2.put(colName2, temp);
	        	continue;
	        }
	        
	        colName = (String)pair.getKey();
	        colNum = tool.getColNum(schema, tableName2, colName);
	        if(colNum ==0){
	        	colNum +=countZ;
	        }else{
	        	colNum += count;
	        }
	        temp = (String) pair.getValue();
	        str = temp.split(",");
	        str[0] = String.valueOf(colNum);
	        temp = str[0]+","+str[1]; 
	        colHashMap2.put(colName, temp); 
	        index++;
	        
	        index++;
	        colNum = tool.getColNum(schema, tableName2, colName)+index;
	        colName = (String)pair.getKey();
	        temp = (String) pair.getValue();
	        str = temp.split(",");
	        str[0] = String.valueOf(index);
	        temp = str[0]+","+str[1]; 
	        
	        colHashMap2.put(colName, temp); 
	    }
	    newSchema.put(tableName2, colHashMap2);
	}
	
	//update two table schema
	public static void createSchema(HashMap<String, HashMap<String, String>> schema,
			String tableName1, String tableName2, String colName1, String colName2){
		
		newSchema = new HashMap<>(createTable.allTable);
	
		HashMap<String, String> colHashMap1 = new HashMap<>();
		HashMap<String, String> colHashMap2 = new HashMap<>();
		
		//find the total number of columns of first table
		int countZ = schema.get(tableName1).size()-1;
		int count = countZ;
		int joinColNum = tool.getColNum(schema, tableName2, colName2);
		if(joinColNum != 0){
			countZ++;
		}
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
	        if(colName2.equals((String)pair.getKey())){
	        	colNum = tool.getColNum(schema, tableName1, colName1);
	        	temp = (String) pair.getValue();
		        str = temp.split(",");
		        str[0] = String.valueOf(colNum);
		        temp = str[0]+","+str[1];
	        	colHashMap2.put(colName2, temp);
	        	continue;
	        }
	        
	        colName = (String)pair.getKey();
	        colNum = tool.getColNum(schema, tableName2, colName);
	        if(colNum ==0){
	        	colNum +=countZ;
	        }else{
	        	colNum += count;
	        }
	        temp = (String) pair.getValue();
	        str = temp.split(",");
	        str[0] = String.valueOf(colNum);
	        temp = str[0]+","+str[1]; 
	        colHashMap2.put(colName, temp); 
	        index++;
	    }
	    newSchema.put(tableName2, colHashMap2);
	}

	public static HashMap<String, HashMap<String, String>> getNewSchema() {
		return newSchema;
	}

}
