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
			ArrayList<StringBuilder> result, String orderQuery,StringBuilder row ){
		//Below ---Extract the TableName,ColName,ColType from tool class for later use.
		String myTableName= "";
		String myColName="";
		String mySortingOrder="";
		if (orderQuery.contains(".")){
			int DotPos = orderQuery.indexOf(".");
			myTableName = orderQuery.substring(0, DotPos);
			String QwithoutTableName = orderQuery.substring(DotPos, orderQuery.length());
			if (QwithoutTableName.contains(" ")){
				int SpacePos = QwithoutTableName.indexOf(" ");
				myColName = QwithoutTableName.substring(1,SpacePos);
			   mySortingOrder = QwithoutTableName.substring(SpacePos+1,QwithoutTableName.length());
			}
		else {
			myColName = QwithoutTableName.substring(1, QwithoutTableName.length());
		}
		}else {
			if (!orderQuery.contains(" ")){
				myColName = orderQuery;
			}else{
				int SpacePos = orderQuery.indexOf(" ");
				myColName = orderQuery.substring(0, SpacePos);
				mySortingOrder = orderQuery.substring(SpacePos+1, orderQuery.length());			
			}	
		}
		int myColPos= tool.getColNum(schema, myTableName, myColName); 
		String myColType= tool.getColType(schema, myTableName, myColName); 
		//Above ---Extract the TableName,ColName,ColType from tool class for later use.
		if (result.size()==0){
			result.add(row);
			return result;
		}
		String[] rowStrings = row.toString().split("\\|");
		for (int i =0;i<result.size();i++){
			String[] tempRow = result.get(i).toString().split("\\|");
			if (!mySortingOrder.contains("desc")){
				if (tool.isLarge(myColType, tempRow[myColPos], rowStrings[myColPos])){
					result.add(i,row);
					return result;
				}else{
					if (i== result.size()-1){
						result.add(row);
						return result;
					}
				}
			}else {
				if (tool.isLarge(myColType, tempRow[myColPos], rowStrings[myColPos])==false){
					result.add(i,row);
					return result;
				}else{
					if (i== result.size()-1){
						result.add(row);
						return result;
					}
				}
			}
		}		
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
