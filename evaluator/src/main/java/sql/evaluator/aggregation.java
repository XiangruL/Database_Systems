package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class aggregation {
	
	//aggregation with groupBy
	public static ArrayList<StringBuilder> aggRow(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result, String aggQuery, String groupQuery,ArrayList<String> tableNames){
		//Ex. aggQuery = "count(author.instituteid)" 
		//Ex. aggQuery = "count(instituteid)";
		//Ex. aggQuery = "count(author.*)" ;
		//Ex. aggQuery = "count(*)";
		
		//parse groupby query, get the groupby table name and column name
		String gtableName;
		String gcolName;
		if(tableNames.size() == 1){
			gtableName = tableNames.get(0);
			gcolName = groupQuery;
		}else{
			String[] gqstr = groupQuery.split("\\.");
			gtableName = gqstr[0];
			gcolName = gqstr[1];
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
			updateRS(schema, result, min(result, acolNum, gcolNum, type), operator);
			break;
	
		case "max":
			updateRS(schema, result, max(result, acolNum, gcolNum, type), operator);
			break;

		default:
			break;
		}
		return result;
	}
	
	//aggregation without any other operation
	public static ArrayList<StringBuilder> aggRow(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result, String aggQuery,ArrayList<String> tableNames){
		//parse the aggQuery get the table name and column name
		String[] qstr = aggQuery.split("\\(");
		String operator = qstr[0];
		String tcName = qstr[1].substring(0,qstr[1].length()-1);
		String tableName = "";
		String colName = "";
		
		if(tableNames.size() == 1){
			tableName = tableNames.get(0);
			colName = tcName;
		}else{
			String[] tcNames = tcName.split("\\.");
			tableName = tcNames[0];
			colName = tcNames[1];
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
			updateRS(schema, operator, result, min(schema, result, colNum, type));
			break;
	
		case "max":
			updateRS(schema, operator, result, max(schema, result, colNum, type));
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
	
	//update result based on group after aggregation
	//update schema
	private static void updateRS(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result,ArrayList<String> posCount,String operator){
		//update result
		int index = 0;
		int begin = 0;
		int count = 0;
		String countResult;
		
		for (int i = 0; i < posCount.size(); i++) {
			if(i == 0){
				updateSchema(schema, operator, result.get(i).toString().split("\\|"));
			}
			
			index = Integer.valueOf(posCount.get(i).split(",")[0])-count;
			countResult = posCount.get(i).split(",")[1];
			result.get(begin).append("|"+ countResult);
			begin++;
			while(begin <= index){
				result.remove(begin);
				index--;
				count++;
				
			}
			begin = index+1;
		}

	}
	
	//update result without group, just remain the frist row
	//update schema
	private static void updateRS(HashMap<String, HashMap<String, String>> schema,
			String operator, ArrayList<StringBuilder> result, String aggResult){
		for (int i = 0; i < result.size(); i++) {
			if(i ==0){
				updateSchema(schema, operator, result.get(i).toString().split("\\|"));
				result.get(i).append("|"+aggResult);
			}else{
				result.remove(1);
			}
			
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
	private static ArrayList<String> max(ArrayList<StringBuilder> result, 
			int acolNum, int gcolNum, String type){
		String max = "";
		String gdataFlag = "";
		String dataFlag = "";
		String[] rowStrings;
		//String is consisted like (the last position,max)
		String posMaxString;
		ArrayList<String> posMax = new ArrayList<>();
		
		for (int i = 0; i < result.size(); i++) {
			rowStrings = result.get(i).toString().split("\\|");
			dataFlag = rowStrings[acolNum];
			
			if(i == 0){
				gdataFlag = rowStrings[gcolNum];
				max = dataFlag;
			}
			
			if(gdataFlag.equals(rowStrings[gcolNum])){
				if(!dataFlag.equalsIgnoreCase("null")){
					if(tool.isSmall(type, max, dataFlag)){
						max = dataFlag;
					}
				}
				if(i == result.size()-1){
					posMaxString = i + "," + max;
					posMax.add(posMaxString);
				}
			}else{
				posMaxString = i-1 + "," + max;
				posMax.add(posMaxString);
				gdataFlag = rowStrings[gcolNum];
				max = dataFlag;
				if(i == result.size()-1){
					posMaxString = i + "," + max;
					posMax.add(posMaxString);
				}
				
			}

		}
		return posMax;
	}
	
	//max without group by
	private static String max(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result, int colNum, String type){
		String max = "";
		String[] resultRow;
		String maxPos = "";
		for (int i = 0 ; i<result.size();i++){
			resultRow = result.get(i).toString().split("\\|");
			if(i==0){
				max = resultRow[colNum];
			}
			if (tool.isSmall(type, max, resultRow[colNum])){
				max = resultRow[colNum];
				maxPos = max+"," +i ;
			}
		}
		return maxPos;
	}
	
	//min with group by
	private static ArrayList<String> min(ArrayList<StringBuilder> result, 
			int acolNum, int gcolNum, String type){
		String min = "";
		String gdataFlag = "";
		String dataFlag = "";
		String[] rowStrings;
		//String is consisted like (the last position,max)
		String posMinString;
		ArrayList<String> posMin = new ArrayList<>();
		
		for (int i = 0; i < result.size(); i++) {
			rowStrings = result.get(i).toString().split("\\|");
			dataFlag = rowStrings[acolNum];
			
			if(i == 0){
				gdataFlag = rowStrings[gcolNum];
				min = dataFlag;
			}
			
			if(gdataFlag.equals(rowStrings[gcolNum])){
				if(!dataFlag.equalsIgnoreCase("null")){
					if(tool.isLarge(type, min, dataFlag)){
						min = dataFlag;
					}
				}
				if(i == result.size()-1){
					posMinString = i + "," + min;
					posMin.add(posMinString);
				}
			}else{
				posMinString = i-1 + "," + min;
				posMin.add(posMinString);
				gdataFlag = rowStrings[gcolNum];
				min = dataFlag;
				if(i == result.size()-1){
					posMinString = i + "," + min;
					posMin.add(posMinString);
				}
				
			}

		}
		return posMin;
	}
	
	//min without group by
	private static String min(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result, int colNum,String type){
		String min = "";
		String[] resultRow;
		String minPos = "";
		for (int i = 0 ; i<result.size();i++){
			resultRow = result.get(i).toString().split("\\|");
			if(i==0){
				min = resultRow[colNum];
			}
			if (tool.isLarge(type, min, resultRow[colNum])){
				min = resultRow[colNum];
				minPos = min+"," +i ;
			}
		}
		return minPos;
	}
}
