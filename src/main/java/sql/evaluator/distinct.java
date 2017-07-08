package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class distinct {
	
	//distinct after projection
	public static ArrayList<StringBuilder> dist(ArrayList<StringBuilder> result){
	
		Map<Integer, String> map = new HashMap<Integer, String>();
		String row;
		for (int i = 0; i < result.size(); i++) {
			row = result.get(i).toString().split(",")[0];
			if(map.containsValue(row)){
				result.remove(i);
				i--;
			}else{
				map.put(i, row);
			}
		}
		return result;
	}
	
	//distinct before projection with result table
	public static ArrayList<StringBuilder> dist(ArrayList<StringBuilder> result,
			HashMap<String, HashMap<String, String>> schema, String query,ArrayList<String> tableNames){
		
		Map<Integer, String> map = new HashMap<Integer, String>();
		String row;
		String[] qstr = query.split(", ");
		String tableName;
		String colName;
		int colNum;
		
		if(tableNames.size() == 1){
			tableName = tableNames.get(0);
			colName = query;
		}else{
			String[] gqstr = query.split("\\.");
			tableName = gqstr[0];
			colName = gqstr[1];
		}
		
		colNum = tool.getColNum(schema, tableName, colName);
		
		
		for (int i = 0; i < result.size(); i++) {
			row = result.get(i).toString().split("\\|")[colNum];
			if(map.containsValue(row)){
				result.remove(i);
				i--;
			}else{
				map.put(i, row);
			}
		}
		return result;
	}
	
	//distinct before projection with row
	//if the given row is already in the result table
	//empty row and return it
	public static StringBuilder dist(ArrayList<StringBuilder> result,
			HashMap<String, HashMap<String, String>> schema, StringBuilder row, String query,
			ArrayList<String> tableNames){
		if(row.length() > 0){
			Map<Integer, String> map = new HashMap<Integer, String>();
			String rowTemp;
			String[] qstr = query.split(", ");
			String tableName;
			String colName;
			int colNum;
			
			if(tableNames.size() == 1){
				tableName = tableNames.get(0);
				colName = query;
			}else{
				String[] gqstr = query.split("\\.");
				tableName = gqstr[0];
				colName = gqstr[1];
			}
			colNum = tool.getColNum(schema, tableName, colName);
			for (int i = 0; i < result.size(); i++) {
				rowTemp = result.get(i).toString().split(",")[0];
				
				if(rowTemp.equals(row.toString().split("\\|")[colNum])){
					row.delete(0, row.length());
					return row;
				}
			}
		}
		return row;
	}
}