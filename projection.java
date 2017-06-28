package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;

public class projection {
	public ArrayList<StringBuilder> proTable(ArrayList<StringBuilder> result,
			HashMap<String, HashMap<String, String>> scheme, 
			StringBuilder row,String query,int index){
		//get table name
		String[] rowString = row.toString().split("\\|");
		String tableName = rowString[rowString.length-1];
		//parse query to get column name
		String[] qstr = query.split(",");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < qstr.length; i++) {
			int colNum = tool.getColNum(scheme, tableName, qstr[i]);
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
		
		return result;
	}

}
