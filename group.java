package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;

public class group {
	
	public static ArrayList<StringBuilder> groupBy(ArrayList<StringBuilder> result,
			HashMap<String, HashMap<String, String>> scheme, StringBuilder row,
			String query){
		if(row.length()>0){
			//if the result contains data
			if(result.size()>0){
				
				//get table name
				//check if contains table name
				
				String tableName;
				String colName;
				if(query.contains(".")){
					String[] qstr = query.split("\\.");
					tableName = qstr[0];
					colName = qstr[1];
				}else{
					
					ArrayList<String> tName = parseSelect.getFromTable();
					if(tName.size()!=1){
						System.err.println("do not specify table name");
						return result;
					}
					tableName = tName.get(0);
					colName = query;
					
				}
			
				//get column number
				int ColNum = tool.getColNum(scheme, tableName, colName);
				String[] resultRow;
				String[] comRow = row.toString().split("\\|");
				int count = 0;
				for (int i = 0; i < result.size(); i++) {
					
					resultRow = result.get(i).toString().split("\\|");
					//check if the given row exists
					
					if(comRow[ColNum].equals(resultRow[ColNum])){
						count++;
					}
					//if the given row does not equal to the result data
					//two condition:
					//1.it equals the former row of the result data
					//2.it doesn't equal the former row of the result
					
					if(!comRow[ColNum].equals(resultRow[ColNum])){
						//if count>0, it equals the former line of the result data
						if(count>0){
							result.add(i, row);
							
							return result;
						}
					}
					//if the given row does not equal all the data in result
					//then add it at the end of the array list
				
					if(i == result.size()-1){
						result.add(row);
						return result;
					}
					
				}
			}else{//if the array list doesn't contain data, add the given row directly
				result.add(row);
			}
		}
		
	
		
		return result;
	}

}
