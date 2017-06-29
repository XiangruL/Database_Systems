package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;

public class projection {
	public static ArrayList<StringBuilder> proTable(ArrayList<StringBuilder> result,
			HashMap<String, HashMap<String, String>> schema, 
			StringBuilder row,String query,int index){
		
		if(row.length()>0){
			//Parse the query, then get table name and column name
			String[] qstr = query.split(", ");
			
			String[] rowString = row.toString().split("\\|");
			String tableName = "";
			String colName = "";
			int colNum;
			StringBuilder sb = new StringBuilder();
			
			for (int i = 0; i < qstr.length; i++) {
				if(qstr[i].contains(".")){
					String[] tcName = qstr[i].split("\\.");
					tableName = tcName[0];
					colName = tcName[1];
				}else{
					ArrayList<String> tName = parseSelect.getFromTable();
					if(tName.size()!=1){
						System.err.println("do not specify table name");
						return result;
					}
					tableName = tName.get(0);
					colName = qstr[i];
				}
				
				colNum = tool.getColNum(schema, tableName, colName);
			
				if(i == qstr.length-1){
					sb.append(rowString[colNum]);
					break;
				}
				sb.append(rowString[colNum]+",");
			}
			
			if(index != -1){
				result.add(index, sb);
			}else{
				result.add(sb);
			}
		}
		
		
		return result;
	}

}
