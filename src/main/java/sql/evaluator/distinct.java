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
			HashMap<String, HashMap<String, String>> schema, String query){
		
		Map<Integer, String> map = new HashMap<Integer, String>();
		String row;
		String[] qstr = query.split(", ");
		String tableName;
		String colName;
		int colNum;
		
		if(qstr[0].contains(".")){
			String[] tcName = qstr[0].split("\\.");
			tableName = tcName[0];
			colName = tcName[1];
		}else{
			ArrayList<String> tName = parseSelect.getFromTable();
			if(tName.size()!=1){
				System.err.println("do not specify table name");
				return result;
			}
			tableName = tName.get(0);
			colName = qstr[0];
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
			HashMap<String, HashMap<String, String>> schema, StringBuilder row, String query){
		if(row.length() > 0){
			Map<Integer, String> map = new HashMap<Integer, String>();
			String rowTemp;
			String[] qstr = query.split(", ");
			String tableName;
			String colName;
			int colNum;
			
			if(qstr[0].contains(".")){
				String[] tcName = qstr[0].split("\\.");
				tableName = tcName[0];
				colName = tcName[1];
			}else{
				ArrayList<String> tName = parseSelect.getFromTable();
				if(tName.size()!=1){
					System.err.println("do not specify table name");
					return row;
				}
				tableName = tName.get(0);
				colName = qstr[0];
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