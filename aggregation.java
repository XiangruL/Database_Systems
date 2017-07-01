package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class aggregation {
	
	//aggregation with groupBy
	public static ArrayList<StringBuilder> aggRow(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result, String aggQuery, String groupQuery){
		StringBuilder sb = new StringBuilder();
		//Ex. aggQuery = "count(author.instituteid)" 
		//Ex. aggQuery = "count(instituteid)";
		//Ex. aggQuery = "count(author.*)" ;
		//Ex. aggQuery = "count(*)";
		
		//parse groupby query, get the groupby table name and column name
		String gtableName;
		String gcolName;
		if(groupQuery.contains(".")){
			String[] gqstr = groupQuery.split("\\.");
			gtableName = gqstr[0];
			gcolName = gqstr[1];
		}else{
			
			ArrayList<String> tName = parseSelect.getFromTable();
			if(tName.size()!=1){
				System.err.println("do not specify table name");
				return result;
			}
			gtableName = tName.get(0);
			gcolName = groupQuery;
			
		}
	
		//parse the aggQuery get the table name and column name
		String[] qstr = aggQuery.split("\\(");
		String operator = qstr[0];
		String tcName = qstr[1].substring(0,qstr[1].length()-1);
		String atableName = "";
		String acolName = "";
		if(tcName.contains("*")){
			acolName = gcolName;
			if(tcName.contains(".")){
				atableName = tcName.split("\\.")[0];
			}else{
				atableName = gtableName;
			}
		}else{
			if(tcName.contains(".")){
				String[] tcNames = tcName.split("\\.");
				atableName = tcNames[0];
				acolName = tcNames[1];
			}else{
				atableName = gtableName;
				acolName = tcName;
			}
		}
		int gcolNum = tool.getColNum(schema, gtableName, gcolName);
		int acolNum = tool.getColNum(schema, atableName, acolName);
		
		switch (operator) {
		case "count":
			
			break;
		
		case "sum":
			String row;
			String[] rowStrings;
			String dataFlag="";
			int sum = 0;
			for (int i = 0; i < result.size(); i++) {
				sb = result.get(i);
				row = sb.toString();
				rowStrings = row.split("\\|");
				if(i ==0){
					dataFlag = rowStrings[gcolNum];
				}
				if(dataFlag.equals(rowStrings[gcolNum])){
					
				}
			}
			
			
			
			break;
			
		case "avg":
	
			break;
	
		case "min":
	
			break;
	
		case "max":
	
			break;

		default:
			break;
		}
		
		
		
		
		
		return result;
	}
	
	//aggregation without any other operation
	public static ArrayList<StringBuilder> aggRow(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result, String aggQuery){
		//parse the aggQuery get the table name and column name
		String[] qstr = aggQuery.split("\\(");
		String operator = qstr[0];
		String tcName = qstr[1].substring(0,qstr[1].length()-1);
		String atableName = "";
		String acolName = "";
		
		if(tcName.contains(".")){
			String[] tcNames = tcName.split("\\.");
			atableName = tcNames[0];
			acolName = tcNames[1];
		}else{
			ArrayList<String> tName = parseSelect.getFromTable();
			if(tName.size()!=1){
				System.err.println("do not specify table name");
				return result;
			}
			atableName = tName.get(0);
			acolName = tcName;
		}
		
		int acolNum = tool.getColNum(schema, atableName, acolName);
		
		switch (operator) {
		case "count":
			
			break;
		
		case "sum":
			String row;
			String[] rowStrings;
			String dataFlag="";
			int sum = 0;
			
			
			
			
			break;
			
		case "avg":
	
			break;
	
		case "min":
	
			break;
	
		case "max":
	
			break;

		default:
			break;
		}				

		
		
		
		
		
		
		
		return result;
	}
	
	
	//update schema
	public static HashMap<String, HashMap<String, String>> updateSchema(HashMap<String, HashMap<String, String>> schema, 
			String colName,String[] row){
		if(schema.containsKey("agg")){
			addNewCol(schema, colName);
		}else{
			addNewTable(schema, colName, row);
		}
		return schema;
	}
	
	//when schema contains "agg" table
	//that means there exist any other aggregation operation before this operation
	//then add a new column in the "agg" table
	public static HashMap<String, HashMap<String, String>> addNewCol(
			HashMap<String, HashMap<String, String>> schema, String colName){
		String colNum = getColNum(schema);
		schema.get("agg").put(colName, colNum);
		return schema;
	}
	
	//when schema doesn't contain "agg" table
	//that means this is the first time do aggregation operation
	//then add a new table named "agg" and add a column in it.
	public static HashMap<String, HashMap<String, String>> addNewTable(
			HashMap<String, HashMap<String, String>> schema, String colName,
			String[] row){
		String colNum = getColNum(row);
		HashMap<String, String> aggMap = new HashMap<>();
		aggMap.put(colName, colNum);
		schema.put("agg", aggMap);		
		return schema;
	}
	
	//"agg" table exist
	//the column number get from the schema
	//check all columns in the "agg" table and find the largest column number of them
	//then the next column = the largest column number + 1
	public static String getColNum(HashMap<String, HashMap<String, String>> schema){
		String colNum = "0";
		Iterator it = schema.get("agg").entrySet().iterator();
		while (it.hasNext()) {
		       Map.Entry pair = (Map.Entry)it.next();
		      
		       if(colNum.compareTo((String)pair.getValue())<0){
		    	   colNum = (String)pair.getValue();
		       }
		      
		}
		int temp = Integer.valueOf(colNum);
		temp++;
		colNum = String.valueOf(temp);
		return colNum;
	}
	
	//"agg" table doesn't exist
	//the column number get from the row length
	public static String getColNum(String[] row){
		String colNum = String.valueOf(row.length);
		return colNum;
	}

}
