package sql.evaluator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class cross {
	
	public static StringBuilder crossProduct(HashMap<String, HashMap<String, String>> oldSchema,
			HashMap<String, HashMap<String, String>> newSchema,String tableName,
			StringBuilder row1, StringBuilder row2, String joinQuery, boolean updateFlag){
		StringBuilder row = new StringBuilder();
		if(row1.length()>0){
			
			if(updateFlag){
				//parse query to get table name and column name
				String[] qstr = joinQuery.split("\\s+");
				String[] tcName1 = qstr[0].split("\\.");
				String[] tcName2 = qstr[2].split("\\.");
				
				String tableName1 = tcName1[0];
				String tableName2 = tcName2[0];
				int index = newSchema.get(tableName1).size()+newSchema.get(tableName2).size()-1;
				updateSchema(oldSchema,newSchema,index,tableName);
				System.out.println("cross:"+newSchema);
			}
			
			row.append(row1.toString()+"|"+row2.toString());
		}
		
		
		return row;
	}
	
	public static void updateSchema(HashMap<String, HashMap<String, String>> oldSchema,
			HashMap<String, HashMap<String, String>> newSchema,int index, String tableName){
		
		HashMap<String, String> colHashMap = new HashMap<>();
		//assign index for the second table column
		String temp = "";
		String[] str;
		Iterator it = oldSchema.get(tableName).entrySet().iterator();
		String colName = null;
		int colNum;
		
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        colName = (String)pair.getKey();
	        colNum = tool.getColNum(oldSchema, tableName, colName);
	     
	        temp = (String) pair.getValue();
	        str = temp.split(",");
	        colNum += index;
	        temp = colNum+","+str[1]; 
	        colHashMap.put(colName, temp); 
	    }
	    newSchema.put(tableName, colHashMap);
	}
}
