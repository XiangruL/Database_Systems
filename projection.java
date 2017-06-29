package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;

public class projection {
	public ArrayList<StringBuilder> proTable(ArrayList<StringBuilder> result,
			HashMap<String, HashMap<String, String>> scheme, 
			StringBuilder row,String query,int index){
		
		if(row.length()>0){
			//Parse the query, then get table name and column name
			String[] qstr = query.split(", ");
			
			String[] rowString = row.toString().split("\\|");
			String tableName;
			String colName;
			int colNum;
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < qstr.length; i++) {
				String[] tcName = qstr[i].split("\\.");
				tableName = tcName[0];
				colName = tcName[1];
			
				colNum = tool.getColNum(scheme, tableName, colName);
			
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
