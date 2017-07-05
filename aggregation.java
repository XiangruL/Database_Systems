package ParseSqlQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import sql.evaluator.parseSelect;

public class aggregation {
	
	//aggregation with groupBy
	public static ArrayList<StringBuilder> aggRow(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result, String aggQuery, String groupQuery){
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
		String type = tool.getColType(schema,atableName, acolName);
		
		switch (operator) {
		case "count":
			updateRS(schema, result, count(result, acolNum, gcolNum), operator);
			
			break;
		
		case "sum":
			updateRS(schema, result, sum(result, acolNum, gcolNum,type), operator);
			
			break;
			
		case "avg":
			updateRS(schema, result, avg(result, acolNum, gcolNum, type), operator);
			break;
	
		case "min":
			min(schema, operator, result, acolNum, gcolNum, type);
			break;
	
		case "max":
			max(schema, operator, result, acolNum, gcolNum, type);
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
		String tableName = "";
		String colName = "";
		
		if(tcName.contains(".")){
			String[] tcNames = tcName.split("\\.");
			tableName = tcNames[0];
			colName = tcNames[1];
		}else{
			ArrayList<String> tName = parseSelect.getFromTable();
			if(tName.size()!=1){
				System.err.println("do not specify table name");
				return result;
			}
			tableName = tName.get(0);
			colName = tcName;
		}
		
		int colNum = tool.getColNum(schema, tableName, colName);
		String type = tool.getColType(schema, tableName, colName);
		switch (operator) {
		case "count":
			updateRS(schema, operator, result, count(result, colNum));
			break;
		
		case "sum":
			updateRS(schema, operator, result, sum(result, colNum, type));
			break;
			
		case "avg":
			updateRS(schema, operator, result, avg(result, colNum, type));
			break;
	
		case "min":
			min(schema, operator, result, colNum, type);
			break;
	
		case "max":
			max(schema, operator, result, colNum, type);
			break;

		default:
			break;
		}				
		return result;
	}
	
	
	//update schema
	private static HashMap<String, HashMap<String, String>> updateSchema(HashMap<String, HashMap<String, String>> schema, 
			String operator,String[] row){
		if(schema.containsKey("agg")){
			addNewCol(schema, operator);
		}else{
			addNewTable(schema, operator, row);
		}
		return schema;
	}
	
	//when schema contains "agg" table
	//that means there exist any other aggregation operation before this operation
	//then add a new column in the "agg" table
	private static HashMap<String, HashMap<String, String>> addNewCol(
			HashMap<String, HashMap<String, String>> schema, String colName){
		String colNum = getColNum(schema);
		schema.get("agg").put(colName, colNum);
		return schema;
	}
	
	//when schema doesn't contain "agg" table
	//that means this is the first time do aggregation operation
	//then add a new table named "agg" and add a column in it.
	private static HashMap<String, HashMap<String, String>> addNewTable(
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
	private static String getColNum(HashMap<String, HashMap<String, String>> schema){
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
	private static String getColNum(String[] row){
		String colNum = String.valueOf(row.length);
		return colNum;
	}
	
	//return result after aggregation
	private static void updateRS(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result,ArrayList<String> posCount,String operator){
		//update result
		int index = 0;
		int begin = 0;
		String countResult;
		for (int i = 0; i < posCount.size(); i++) {
			if(i == 0){
				updateSchema(schema, operator, result.get(i).toString().split("\\|"));
			}
			index = Integer.valueOf(posCount.get(i).split(",")[0]);
			countResult = posCount.get(i).split(",")[1];
			
			for (int j = begin; j <= index; j++) {
				result.get(j).append("|"+ countResult);
				
			}
			begin = index+1;
		}
	}
	
	//update schema and result
	private static void updateRS(HashMap<String, HashMap<String, String>> schema,
			String operator, ArrayList<StringBuilder> result, String aggResult){
		for (int i = 0; i < result.size(); i++) {
			if(i ==0){
				updateSchema(schema, operator, result.get(i).toString().split("\\|"));
			}
			result.get(i).append("|"+aggResult);
		}
	}
	
	//count with group by
	private static ArrayList<String> count(ArrayList<StringBuilder> result, int acolNum, int gcolNum){
		int count = 0;
		String gdataFlag = "";
		String dataFlag = "";
		String[] rowStrings;
		//String is consisted like (the last position,count)
		String posCouString;
		ArrayList<String> posCount = new ArrayList<>();
		
		for (int i = 0; i < result.size(); i++) {
			rowStrings = result.get(i).toString().split("\\|");
			dataFlag = rowStrings[acolNum];
			
			if(i == 0){
				gdataFlag = rowStrings[gcolNum];
			}
			
			if(gdataFlag.equals(rowStrings[gcolNum])){
				if(!dataFlag.equalsIgnoreCase("null")){
					count++;
				}
				if(i == result.size()-1){
					posCouString = i + "," + count;
					posCount.add(posCouString);
				}
			}else{
				posCouString = i-1 + "," + count;
				posCount.add(posCouString);
				gdataFlag = rowStrings[gcolNum];
				count = 1;
				if(i == result.size()-1){
					posCouString = i + "," + count;
					posCount.add(posCouString);
				}
				
			}

		}
		return posCount;
	}
	
	//count without group by
	private static String count(ArrayList<StringBuilder> result, int colNum){
		int count = 0;
		String[] rowStrings;
		String aggResult;
		for (int i = 0; i < result.size(); i++) {
			rowStrings = result.get(i).toString().split("\\|");
			if(!(rowStrings[colNum].equalsIgnoreCase("null"))){
				count++;
			}
		}
		aggResult = String.valueOf(count);
		return aggResult;
	}
	
	//sum with group by
	private static ArrayList<String> sum(ArrayList<StringBuilder> result, 
			int acolNum, int gcolNum, String type){
		String[] rowStrings;
		String posSumString;
		String dataFlag = "";
		ArrayList<String> posSum = new ArrayList<>();
		String gdataFlag = "";
		double sum = 0.0;
		String str;
		//double
		for (int i = 0; i < result.size(); i++) {
			rowStrings = result.get(i).toString().split("\\|");
			dataFlag = rowStrings[acolNum];
			if(i ==0){
				gdataFlag = rowStrings[gcolNum];
			}
			if(gdataFlag.equals(rowStrings[gcolNum])){
				if(!dataFlag.equalsIgnoreCase("null")){
					sum+= Double.valueOf(dataFlag);	
				}
				if(i == result.size()-1){
					str = String.valueOf(sum);
					if(type.equals("integer")){
						posSumString = i + "," + str.split("\\.")[0];
					}else{
						posSumString = i + "," + sum;
					}
					posSum.add(posSumString);
				}
			}else{
				str = String.valueOf(sum);
				if(type.equals("integer")){
					posSumString = i-1 + "," + str.split("\\.")[0];
				}else{
					posSumString = i-1+","+sum;
				}
				
				posSum.add(posSumString);
				sum = Double.valueOf(dataFlag);
				gdataFlag = rowStrings[gcolNum];
				if(i == result.size()-1){
					if(!dataFlag.equalsIgnoreCase("null")){
						str = String.valueOf(sum);
						if(type.equals("integer")){
							posSumString = i + "," + str.split("\\.")[0];
						}else{
							posSumString = i + "," + sum;
						}
						posSum.add(posSumString);
					}
					
				}
			}
		}
		return posSum;
	}
	
	//sum without group by
	private static String sum(ArrayList<StringBuilder> result, int colNum, String type){
		double sum = 0;
		String[] rowStrings;
		String aggResult;
		for (int i = 0; i < result.size(); i++) {
			rowStrings = result.get(i).toString().split("\\|");
			if(!(rowStrings[colNum].equalsIgnoreCase("null"))){
				sum+=Double.valueOf(rowStrings[colNum]);
			}
		}
		aggResult=String.valueOf(sum);
		if(type.equals("integer")){
			aggResult = aggResult.split("\\.")[0];
		}
		return aggResult;
	}
	
	//avg with group by
	private static ArrayList<String> avg(ArrayList<StringBuilder> result, int acolNum, int gcolNum,String type){
		String posAvgString;
		ArrayList<String> posAvg = new ArrayList<>();
		ArrayList<String> posCount = count(result, acolNum, gcolNum);
		ArrayList<String> posSum = sum(result, acolNum, gcolNum,type);
		double avg;
		String countResult;
		String sumResult;
		for (int i = 0; i < posSum.size(); i++) {
			countResult = posCount.get(i).split(",")[1];
			sumResult =  posSum.get(i).split(",")[1];
			avg = (Double.valueOf(sumResult))/(Double.valueOf(countResult));
			posAvgString = posSum.get(i).split(",")[0]+","+avg;
			posAvg.add(posAvgString);
		}
		return posAvg;
	}
	
	//avg without group by
	private static String avg(ArrayList<StringBuilder> result, int colNum,String type){
		int count = Integer.valueOf(count(result, colNum));
		double sum = Double.valueOf(sum(result, colNum, type));
		double avg = sum/count;
		String str = String.valueOf(avg);
		return str;
	}
	
		
	//max with group by
	private static void max(HashMap<String, HashMap<String, String>> schema,
			String operator, ArrayList<StringBuilder> result, int acolNum, int gcolNum, String type){
		int oriLen2 = result.get(0).toString().split("\\|").length;
		String [] firstRec1max = result.get(0).toString().split("\\|");
		String record1max =firstRec1max[gcolNum] ;
		String record111 = firstRec1max[acolNum] ;
		String answer111 = firstRec1max[acolNum] + "|" + "0";
		ArrayList<String> posCount1=  new ArrayList<String>();
		String[] incomingRow;
		for (int i = 0 ; i<result.size();i++){
			incomingRow = result.get(i).toString().split("\\|");
			if(i==0){
				updateSchema(schema,operator,incomingRow);
					
			}
			if (!incomingRow[acolNum].toLowerCase().equals("null")){
				
			
			
			if (result.get(i).toString().split("\\|")[gcolNum].equals(record1max)){
				if (incomingRow[acolNum].equals(record111)){
					answer111 = answer111 + "|" + i; 
				}
				else if (tool.isLarge(type, incomingRow[acolNum], record111)){
					record111 = incomingRow[acolNum];
					answer111 = incomingRow[acolNum]+"|" +i ;
				}
			}
			else{
				int currentpos = i-1;
				if (currentpos!= -1){
					posCount1.add(answer111);
				}
				record111 = result.get(i).toString().split("\\|")[acolNum];
				answer111 = result.get(i).toString().split("\\|")[acolNum]+ "|" + i;
				record1max = result.get(i).toString().split("\\|")[gcolNum];
			}
			if (i==result.size()-1 ){
				posCount1.add( answer111);
			}
			
			}
			
		}

		
		for (int i =0; i< posCount1.size();i++){
			String addb [] = posCount1.get(i).split("\\|");
				for ( int j=1; j<2;j++){
					StringBuilder mysb = new StringBuilder();
					mysb.append("|" +addb[0]);
					result.get(Integer.valueOf(addb[j])).append(mysb);
				}
				for ( int j=3; j<addb.length;j++){
					StringBuilder mysb = new StringBuilder();
					mysb.append("|" +addb[0]);
					result.get(Integer.valueOf(addb[j])).append(mysb);
				}
	}
		StringBuilder mynull2 = new StringBuilder();
		mynull2.append("|"  +"null");
		
		for ( int i =0 ; i< result.size();i++){
			String [] temp = result.get(i).toString().split("\\|");
			if (temp.length==oriLen2){
				result.get(i).append(mynull2);

			}
		}
	}
	
	//max without group by
	private static void max(HashMap<String, HashMap<String, String>> schema,
			String operator, ArrayList<StringBuilder> result, int acolNum, String type){
		int oriLen1 = result.get(0).toString().split("\\|").length;
		String [] firstRec11 = result.get(0).toString().split("\\|");
		String record11 =firstRec11[acolNum] ;
		String answer11 = firstRec11[acolNum];
		String[] incomingRow;
		for (int i = 0 ; i<result.size();i++){
			incomingRow = result.get(i).toString().split("\\|");
			if (!incomingRow[acolNum].toLowerCase().equals("null")){
			
			if(i==0){
				updateSchema(schema,operator,incomingRow);
				
			}
			if (tool.isLarge(type, incomingRow[acolNum], record11)){
				record11 = incomingRow[acolNum];				
				answer11 = incomingRow[acolNum]+"|" +i ;
			}else if (incomingRow[acolNum].equals(record11)){
				answer11 = answer11 + "|" + i; 
			}			
			
			}
			
		}
		StringBuilder mysb1 = new StringBuilder();
		mysb1.append("|"  +record11);
		StringBuilder mynull1 = new StringBuilder();
		mynull1.append("|"  +"null");
		String[] addBack11 = answer11.split("\\|");
		for (int i =1 ; i<addBack11.length;i++){
			result.get(Integer.valueOf(addBack11[i])).append(mysb1);
		}
		for ( int i =0 ; i< result.size();i++){
			String [] temp = result.get(i).toString().split("\\|");
			if (temp.length==oriLen1){
				result.get(i).append(mynull1);
			}
		}
	}
	
	//min with group by
	private static void min(HashMap<String, HashMap<String, String>> schema,
			String operator, ArrayList<StringBuilder> result, int acolNum, int gcolNum, String type){
		int oriLen = result.get(0).toString().split("\\|").length;
		String [] firstRec1 = result.get(0).toString().split("\\|");
		String record1 =firstRec1[gcolNum] ;
		String record11 = firstRec1[acolNum] ;
		String answer11 = firstRec1[acolNum] + "|" + "0";
		ArrayList<String> posCount=  new ArrayList<String>();
		String[] incomingRow;

		for (int i = 0 ; i<result.size();i++){
			incomingRow=result.get(i).toString().split("\\|");
			if(i==0){
				updateSchema(schema,operator,incomingRow);	
			}
			
			if (!incomingRow[acolNum].toLowerCase().equals("null")){
			if (incomingRow[gcolNum].equals(record1)){
				if (incomingRow[acolNum].equals(record11)){
					answer11 = answer11 + "|" + i; 
				}
				else if (tool.isSmall(type, incomingRow[acolNum], record11)){
					record11 = incomingRow[acolNum];
					answer11 = incomingRow[acolNum]+"|" +i ;
				}
			}
			
			else{
				int currentpos = i-1;
				if (currentpos!= -1){
				posCount.add( answer11);
			}

				record11 = result.get(i).toString().split("\\|")[acolNum];
				answer11 = result.get(i).toString().split("\\|")[acolNum]+ "|" + i;
				record1 = result.get(i).toString().split("\\|")[gcolNum];
			}
			if (i==result.size()-1 ){
				posCount.add( answer11);
				
			}
		}
		}
		
		for (int i =0; i< posCount.size();i++){
			String addb [] = posCount.get(i).split("\\|");
				for ( int j=1; j<2;j++){
					
					StringBuilder mysb = new StringBuilder();
					mysb.append("|" +addb[0]); 
					result.get(Integer.valueOf(addb[j])).append(mysb);
				}
				for ( int j=3; j<addb.length;j++){
					
					StringBuilder mysb = new StringBuilder();
					mysb.append("|" +addb[0]); 
					result.get(Integer.valueOf(addb[j])).append(mysb);
				}

		}
		StringBuilder mynull = new StringBuilder();
		mynull.append("|"  +"null");
		
		for ( int i =0 ; i< result.size();i++){
			String [] temp = result.get(i).toString().split("\\|");
			if (temp.length==oriLen){
				result.get(i).append(mynull);

			}
		}		
	}
	
	//min without group by
	private static void min(HashMap<String, HashMap<String, String>> schema,
			String operator, ArrayList<StringBuilder> result, int acolNum,String type){
		int oriLen = result.get(0).toString().split("\\|").length;
		String [] firstRec1 = result.get(0).toString().split("\\|");
		String record1 =firstRec1[acolNum] ;
		String answer1 = firstRec1[acolNum];
		String[] incomingRow;
		for (int i = 0 ; i<result.size();i++){
			
			incomingRow = result.get(i).toString().split("\\|");
			
			if (!incomingRow[acolNum].toLowerCase().equals("null")){
			if(i==0){
				updateSchema(schema,operator,incomingRow);
			}
			if (tool.isSmall(type, incomingRow[acolNum], record1)){
				record1 = incomingRow[acolNum];
				answer1 = incomingRow[acolNum]+"|" +i ;
			}
			else if (incomingRow[acolNum].equals(record1)){
				answer1 = answer1 + "|" + i; 
			}
			}
		}
		StringBuilder mysb = new StringBuilder();
		mysb.append("|"  +record1);
		StringBuilder mynull = new StringBuilder();
		mynull.append("|"  +"null");
		String[] addBack1 = answer1.split("\\|");
		for (int i =1 ; i<addBack1.length;i++){
			result.get(Integer.valueOf(addBack1[i])).append(mysb);
		}

		for ( int i =0 ; i< result.size();i++){
			String [] temp = result.get(i).toString().split("\\|");
			if (temp.length==oriLen){
				result.get(i).append(mynull);
			}
		}
	}
}
