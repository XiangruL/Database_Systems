package ParseSqlQuery;

import java.util.HashMap;

public class tool {
	
	public static int getColNum(HashMap<String, HashMap<String, String>> allTable,
			String tableName, String colName){
		//get the column information
		HashMap<String, String> tableMap1 = allTable.get(tableName);
						
		//get the column number and type
		String col1 = tableMap1.get(colName);
						
		//split the column number and type into an array
		String[] str1 = col1.split(",");
				
		//get the column number, cast to Integer;
		int colNum = Integer.valueOf(str1[0]);
		
		return colNum;
	}

}
