package ParseSqlQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import sql.evaluator.parseSelect;

public class aggV2 {
	
	//aggV2 with groupBy
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
		
		switch (operator) {
		case "count":
			updateResult(result, count(schema,operator,result, acolNum, gcolNum));
			
			break;
		
		case "sum":
			updateResult(result, sum(schema,operator,result, acolNum, gcolNum));
			
			break;
			
		case "avg":
			updateResult(schema, operator, result, acolNum, gcolNum);
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
	
	//aggV2 without any other operation
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
		String aclotype = tool.getColType(schema, atableName, acolName);
		switch (operator) {
		case "count":
			
			break;
		
		case "sum":
			
			 
			
			
			
			break;
			
		case "avg":
	
			break;
	
		case "min":
			String [] firstRec = result.get(0).toString().split("\\|");
			String record =firstRec[acolNum] ;
			String answer = firstRec[acolNum] + "|" + "0";
			for (int i = 1 ; i<result.size();i++){
				String[] incomingRow = result.get(i).toString().split("\\|");
				
				if (tool.isLarge(aclotype, incomingRow[acolNum], record)){

					
				}
				else if (incomingRow[acolNum].equals(record)){
					answer = answer + "|" + i; 
				}
				
				else{
					
					record = incomingRow[acolNum];
					
					answer = incomingRow[acolNum]+"|" +i ;
				}
			}
			System.out.println(record);
			System.out.println(answer);
			String[] addBack = answer.split("\\|");
			for (int i =1 ; i<addBack.length;i++){
				
				System.out.println(addBack[i]);
				StringBuilder mysb = new StringBuilder();
				mysb.append("|"  +record);
				result.get(Integer.valueOf(addBack[i])).append(mysb);
			}

			break;
	
		case "max":
			String [] firstRec1 = result.get(0).toString().split("\\|");
			String record1 =firstRec1[acolNum] ;
			String answer1 = firstRec1[acolNum] + "|" + "0";
			for (int i = 1 ; i<result.size();i++){
				String[] incomingRow = result.get(i).toString().split("\\|");
				
				if (tool.isLarge(aclotype, incomingRow[acolNum], record1)==false){

					
				}
				else if (incomingRow[acolNum].equals(record1)){
					answer1 = answer1 + "|" + i; 
				}
				
				else{
					
					record1 = incomingRow[acolNum];
					
					answer1 = incomingRow[acolNum]+"|" +i ;
				}
			}
			System.out.println(record1);
			System.out.println(answer1);
			String[] addBack1 = answer1.split("\\|");
			for (int i =1 ; i<addBack1.length;i++){
				
				System.out.println(addBack1[i]);
				StringBuilder mysb = new StringBuilder();
				mysb.append("|"  +record1);
				result.get(Integer.valueOf(addBack1[i])).append(mysb);
			}

			break;

		default:
			break;
		}				

		
		
		
		
		
		
		
		return result;
	}
	
	
	//update schema
	public static HashMap<String, HashMap<String, String>> updateSchema(HashMap<String, HashMap<String, String>> schema, 
			String operator,String[] row){
		if(schema.containsKey("agg")){
			addNewCol(schema, operator);
		}else{
			addNewTable(schema, operator, row);
		}
		return schema;
	}
	
	//when schema contains "agg" table
	//that means there exist any other aggV2 operation before this operation
	//then add a new column in the "agg" table
	public static HashMap<String, HashMap<String, String>> addNewCol(
			HashMap<String, HashMap<String, String>> schema, String colName){
		String colNum = getColNum(schema);
		schema.get("agg").put(colName, colNum);
		return schema;
	}
	
	//when schema doesn't contain "agg" table
	//that means this is the first time do aggV2 operation
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
	
	//return result after aggV2
	public static void updateResult(ArrayList<StringBuilder> result,ArrayList<String> posCount){
		//update result
		int index = 0;
		int begin = 0;
		String countResult;
		for (int i = 0; i < posCount.size(); i++) {
			index = Integer.valueOf(posCount.get(i).split(",")[0]);
			countResult = posCount.get(i).split(",")[1];
			
			for (int j = begin; j <= index; j++) {
				result.get(j).append("|"+ countResult);
				
			}
			begin = index+1;
		}
		

	}
	
	//return avg result after aggV2
	public static void updateResult(HashMap<String, HashMap<String, String>> schema,
			String operator, ArrayList<StringBuilder> result, int acolNum, int gcolNum){
		String[] rowStrings;
		String posAvgString;
		ArrayList<String> posAvg = new ArrayList<>();
		String gdataFlag = "";
		String dataFlag = "";
		ArrayList<String> posCount = count(result, acolNum, gcolNum);
		ArrayList<String> posSum = sum(result, acolNum, gcolNum);
		//update result
		int index = 0;
		int begin = 0;
		double avg;
		String countResult;
		String sumResult;
		
		for (int i = 0; i < posSum.size(); i++) {
			if(i ==0){
				rowStrings = result.get(i).toString().split("\\|");
				updateSchema(schema, operator, rowStrings);
			}
			index = Integer.valueOf(posSum.get(i).split(",")[0]);
			countResult = posCount.get(i).split(",")[1];
			sumResult =  posSum.get(i).split(",")[1];
			avg = (Double.valueOf(sumResult))/(Double.valueOf(countResult));
			for (int j = begin; j <= index; j++) {
				result.get(j).append("|"+ avg);
				
			}
			begin = index+1;
		}
	}
	
	
	//with updating schema
	public static ArrayList<String> count(HashMap<String, HashMap<String, String>> schema,
			String operator, ArrayList<StringBuilder> result, int acolNum, int gcolNum){
		int count = 0;
		String gdataFlag = "";
		String dataFlag = "";
		String[] rowStrings;
		//String is consisted like (the last position,count)
		String posCouString;
		ArrayList<String> posCount = new ArrayList<>();
		
		if(gcolNum == acolNum){//the column which is grouby and count is the same column
			for (int i = 0; i < result.size(); i++) {
				rowStrings = result.get(i).toString().split("\\|");
				
				//assign dataFlag
				//update schema
				if(i==0){
					dataFlag = rowStrings[acolNum];
					updateSchema(schema,operator,rowStrings);
				}
				if(dataFlag.equals(rowStrings[acolNum])){
					count++;
					if(i == result.size()-1){
						posCouString = i + "," + count;
						posCount.add(posCouString);
					}
				}else{
					posCouString = i-1 + "," + count;
					posCount.add(posCouString);
					dataFlag = rowStrings[acolNum];
					count = 1;
					if(i == result.size()-1){
						posCouString = i + "," + count;
						posCount.add(posCouString);
					}
				}
				
			}
			
		}else{//the column which is grouby and count is not the same column
			
			for (int i = 0; i < result.size(); i++) {
				rowStrings = result.get(i).toString().split("\\|");
				dataFlag = rowStrings[acolNum];
				
				if(i == 0){
					gdataFlag = rowStrings[gcolNum];
					updateSchema(schema,operator,rowStrings);
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

		}
		return posCount;
	}
	
	//without updating schema
	public static ArrayList<String> count(ArrayList<StringBuilder> result, int acolNum, int gcolNum){
		int count = 0;
		String gdataFlag = "";
		String dataFlag = "";
		String[] rowStrings;
		//String is consisted like (the last position,count)
		String posCouString;
		ArrayList<String> posCount = new ArrayList<>();
		
		if(gcolNum == acolNum){//the column which is grouby and count is the same column
			for (int i = 0; i < result.size(); i++) {
				rowStrings = result.get(i).toString().split("\\|");
				
				//assign dataFlag
				//update schema
				if(i==0){
					dataFlag = rowStrings[acolNum];
				}
				if(dataFlag.equals(rowStrings[acolNum])){
					count++;
					if(i == result.size()-1){
						posCouString = i + "," + count;
						posCount.add(posCouString);
					}
				}else{
					posCouString = i-1 + "," + count;
					posCount.add(posCouString);
					dataFlag = rowStrings[acolNum];
					count = 1;
					if(i == result.size()-1){
						posCouString = i + "," + count;
						posCount.add(posCouString);
					}
				}
				
			}
			
		}else{//the column which is grouby and count is not the same column
			
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

		}
		return posCount;
	}
	
	//with updating schema
	public static ArrayList<String> sum(HashMap<String, HashMap<String, String>> schema,
			String operator, ArrayList<StringBuilder> result, int acolNum, int gcolNum){
		String[] rowStrings;
		String posSumString;
		String dataFlag = "";
		ArrayList<String> posSum = new ArrayList<>();
		String gdataFlag = "";
		double sum = 0.0;
		//double
		for (int i = 0; i < result.size(); i++) {
			rowStrings = result.get(i).toString().split("\\|");
			dataFlag = rowStrings[acolNum];
			if(i ==0){
				gdataFlag = rowStrings[gcolNum];
				updateSchema(schema,operator,rowStrings);
			}
			if(gdataFlag.equals(rowStrings[gcolNum])){
				if(!dataFlag.equalsIgnoreCase("null")){
					sum+= Double.valueOf(dataFlag);	
				}
				if(i == result.size()-1){
					posSumString = i + "," + sum;
				}
			}else{
				posSumString = i-1+","+sum;
				posSum.add(posSumString);
				sum = Double.valueOf(dataFlag);
				gdataFlag = rowStrings[gcolNum];
				if(i == result.size()-1){
					if(!dataFlag.equalsIgnoreCase("null")){
						
						posSumString = i + "," + sum;
						posSum.add(posSumString);
					}
					
				}
			}
		}
		return posSum;
	}
	
	//without updating schema
	public static ArrayList<String> sum(ArrayList<StringBuilder> result, int acolNum, int gcolNum){
		String[] rowStrings;
		String posSumString;
		String dataFlag = "";
		ArrayList<String> posSum = new ArrayList<>();
		String gdataFlag = "";
		double sum = 0.0;
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
					posSumString = i + "," + sum;
				}
			}else{
				posSumString = i-1+","+sum;
				posSum.add(posSumString);
				sum = Double.valueOf(dataFlag);
				gdataFlag = rowStrings[gcolNum];
				if(i == result.size()-1){
					if(!dataFlag.equalsIgnoreCase("null")){
						
						posSumString = i + "," + sum;
						posSum.add(posSumString);
					}
					
				}
			}
		}
		return posSum;
	}

}
