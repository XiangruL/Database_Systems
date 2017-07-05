package sql.evaluator;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import sql.evaluator.parseSelect;
//score desc
//score ASC
//P.pubTypeID
//P.pubTypeID desc
//P.pubTypeID ASC
//P.desc desc
//P.ASC ASC

public class order{

	public static ArrayList<StringBuilder> orderBy(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result, StringBuilder row, String orderQuery){
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
				ArrayList<String> tName = parseSelect.getFromTable();
				if(tName.size()!=1){
					System.err.println("do not specify table name");
					return result;
				}
				otableName = tName.get(0);
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
				
				ArrayList<String> tName = parseSelect.getFromTable();
				if(tName.size()!=1){
					System.err.println("do not specify table name");
					return result;
				}
				otableName = tName.get(0);
				ocolName = oqstr[0];
			}
		}else{
			order = "asc";
			if(orderQuery.contains(".")){
				String[] qstr = orderQuery.split("\\.");
				otableName = qstr[0];
				ocolName = qstr[1];
			}else{
				
				ArrayList<String> tName = parseSelect.getFromTable();
				if(tName.size()!=1){
					System.err.println("do not specify table name");
					return result;
				}
				otableName = tName.get(0);
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
	
	public static ArrayList<StringBuilder> orderBy(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result, StringBuilder row, String orderQuery, String groupQuery ){
		
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
			ArrayList<String> tName = parseSelect.getFromTable();
			if(tName.size()!=1){
				System.err.println("do not specify table name");
				return result;
			}
			gtableName = tName.get(0);
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
				ArrayList<String> tName = parseSelect.getFromTable();
				if(tName.size()!=1){
					System.err.println("do not specify table name");
					return result;
				}
				otableName = tName.get(0);
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
				
				ArrayList<String> tName = parseSelect.getFromTable();
				if(tName.size()!=1){
					System.err.println("do not specify table name");
					return result;
				}
				otableName = tName.get(0);
				ocolName = oqstr[0];
			}
		}else{
			order = "asc";
			if(orderQuery.contains(".")){
				String[] qstr = orderQuery.split("\\.");
				otableName = qstr[0];
				ocolName = qstr[1];
			}else{
				
				ArrayList<String> tName = parseSelect.getFromTable();
				if(tName.size()!=1){
					System.err.println("do not specify table name");
					return result;
				}
				otableName = tName.get(0);
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
}
