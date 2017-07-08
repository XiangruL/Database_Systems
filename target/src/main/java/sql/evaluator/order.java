package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;

public class order{
	
	//doing order by without group by
	//receiving a row and insert it into the array list with the correct order
	public static ArrayList<StringBuilder> orderBy(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result, StringBuilder row, String orderQuery, 
			ArrayList<String> tableNames){
		if(row.length() == 0){
			return result;
		}
		//parse order by query
		String otableName = null;
		String ocolName = null;
		String order;
		String[] oqstr = null;
		
		if(orderQuery.contains("desc")){
			oqstr = orderQuery.split("\\s+");
			order = "desc";
			if(oqstr[0].contains(".")){
				String[] qstr = oqstr[0].split("\\.");
				otableName = qstr[0];
				ocolName = qstr[1];
			}else{
				otableName = tableNames.get(0);
				ocolName = oqstr[0];
			}
		}else if(orderQuery.contains("asc")){
			oqstr = orderQuery.split("\\s+");
			order = "asc";
			if(oqstr[0].contains(".")){
				String[] qstr = oqstr[0].split("\\.");
				otableName = qstr[0];
				ocolName = qstr[1];
			}else{
				otableName =  tableNames.get(0);;
				ocolName = oqstr[0];
			}
		}else{
			order = "asc";
			if(orderQuery.contains(".")){
				String[] qstr = orderQuery.split("\\.");
				otableName = qstr[0];
				ocolName = qstr[1];
			}else{
				otableName = tableNames.get(0);;
				ocolName = orderQuery;
			}
		}
		//get order by column number and type
		int oColNum = tool.getColNum(schema, otableName, ocolName);
		String otype = tool.getColType(schema, otableName, ocolName);
	
		String[] resultStrings;
		String[] rowStrings = row.toString().split("\\|");
		for (int i = 0; i < result.size(); i++) {
			resultStrings = result.get(i).toString().split("\\|");
			if(order.equals("asc")){
				if(tool.isSmall(otype, rowStrings[oColNum], resultStrings[oColNum])){
					result.add(i,row);
					return result;
				}
			}else{
				if(tool.isLarge(otype, rowStrings[oColNum], resultStrings[oColNum])){
					result.add(i,row);
					return result;
				}
			}
		}
		result.add(row);
		return result;
}
	
	//order by with group by
	public static ArrayList<StringBuilder> orderBy(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result, StringBuilder row, String orderQuery, String groupQuery,
			ArrayList<String> tableNames){
		
		if(row.length() == 0){
			return result;
		}
		//parse group query
		String gtableName;
		String gcolName;
		if(groupQuery.contains(".")){
			String[] qstr = groupQuery.split("\\.");
			gtableName = qstr[0];
			gcolName = qstr[1];
		}else{
			gtableName = tableNames.get(0);
			gcolName = groupQuery;
		}
		//parse order by query
		String otableName = null;
		String ocolName = null;
		String order;
		String[] oqstr = null;
		if(orderQuery.contains("desc")){
			oqstr = orderQuery.split("\\s+");
			order = "desc";
			if(oqstr[0].contains(".")){
				String[] qstr = oqstr[0].split("\\.");
				otableName = qstr[0];
				ocolName = qstr[1];
			}else{
				otableName = tableNames.get(0);
				ocolName = oqstr[0];
			}
		}else if(orderQuery.contains("asc")){
			oqstr = orderQuery.split("\\s+");
			order = "asc";
			if(oqstr[0].contains(".")){
				String[] qstr = oqstr[0].split("\\.");
				otableName = qstr[0];
				ocolName = qstr[1];
			}else{
				otableName = tableNames.get(0);
				ocolName = oqstr[0];
			}
		}else{
			order = "asc";
			if(orderQuery.contains(".")){
				String[] qstr = orderQuery.split("\\.");
				otableName = qstr[0];
				ocolName = qstr[1];
			}else{
				otableName = tableNames.get(0);
				ocolName = orderQuery;
			}
		}
		//get group column number
		int gColNum = tool.getColNum(schema, gtableName, gcolName);
		//get order by column number and type
		int oColNum = tool.getColNum(schema, otableName, ocolName);
		String otype = tool.getColType(schema, otableName, ocolName);
		
		boolean gourpEndFlag = false;
		boolean insertSucFlag = false;
		String[] resultStrings;
		String[] rowStrings = row.toString().split("\\|");
		for (int i = 0; i < result.size(); i++) {
			resultStrings = result.get(i).toString().split("\\|");
			if(resultStrings[gColNum].equals(rowStrings[gColNum])){
				gourpEndFlag = true;
				if(order.equals("asc")){
					if(tool.isSmall(otype, rowStrings[oColNum], resultStrings[oColNum])&&!insertSucFlag){
						result.add(i,row);
						insertSucFlag = true;
						i++;
					}
				}else{
					if(tool.isLarge(otype, rowStrings[oColNum], resultStrings[oColNum])&&!insertSucFlag){
						result.add(i,row);
						insertSucFlag = true;
						i++;
					}
				}
				if(i == result.size()-1 && insertSucFlag){
					result.remove(i);
					return result;
				}
			}else if(gourpEndFlag){
				if(insertSucFlag){
					result.remove(i-1);
					return result;
				}
			}
		}
		return result;
	}
	
	//order by the result set
	public static ArrayList<StringBuilder> orderBy(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result, String orderQuery, ArrayList<String> tableNames){
		//parse the aggregation type to get the column number
		String[] qstr = orderQuery.split("\\(");
		int colNum = tool.getColNum(schema, "agg", qstr[0]);
		String order = "";
		if(orderQuery.contains("desc")){
			order = "desc";
		}else{
			order = "asc";
		}
		
		double num1;
		double num2;
		StringBuilder temp;
		for (int i = 0; i < result.size(); i++) {
			
			if(order.equals("asc")){
				for (int j = i; j >0; j--) {
					num1 = Double.valueOf(result.get(j).toString().split("\\|")[colNum]);
					num2 = Double.valueOf(result.get(j-1).toString().split("\\|")[colNum]);
					
					if(num1<num2){
						temp = result.get(j);
						result.add(j-1, result.get(j));
						result.remove(j+1);
						
					}
				}
			}else{
				for (int j = i; j >0; j--) {
					num1 = Double.valueOf(result.get(j).toString().split("\\|")[colNum]);
					num2 = Double.valueOf(result.get(j-1).toString().split("\\|")[colNum]);
					
					if(num1>num2){
						temp = result.get(j);
						result.add(j-1, result.get(j));
						result.remove(j+1);
						
					}
				}
			}
		}

		return result;
	}
	
	public static void swap(StringBuilder row1, StringBuilder row2){
		StringBuilder temp = row1;
		row1 = row2;
		row2 = temp;
		System.out.println("000"+row1);
		System.out.println("000"+row2);
	}
}
