package sql.evaluator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class cross {
	
	public static StringBuilder crossProduct(HashMap<String, HashMap<String, String>> oldSchema,
			HashMap<String, HashMap<String, String>> newSchema,String tableName,
			StringBuilder row1, StringBuilder row2, boolean updateFlag){
		StringBuilder row = new StringBuilder();
		if(row1.length()>0&&row2.length()>0){
			
			if(updateFlag){
				int index = join.index+1;
				
				updateSchema(oldSchema,newSchema,index,tableName);
			
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
