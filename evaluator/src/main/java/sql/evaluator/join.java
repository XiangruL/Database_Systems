package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class join {
	static HashMap<String, HashMap<String, String>> newSchema = null;
	static int index = 0;
	
	public static StringBuilder joinRow(HashMap<String, HashMap<String, String>> schema,
			HashMap<String, HashMap<String, String>> oldSchema, String tableName,
			StringBuilder row1, StringBuilder row2, String query, boolean flag, boolean updatFlag){
		
		StringBuilder row = new StringBuilder();
		if(row1.length()>0&&row2.length()>0){
			
			row.append(row1.toString());
			String[] rowString1 = row.toString().split("\\|");
			String[] rowString2 = row2.toString().split("\\|");
			
			//parse query to get table name and column name
			String[] qstr = query.split("\\s+");
			String[] tcName1 = qstr[0].split("\\.");
			String[] tcName2 = qstr[2].split("\\.");
			
			String tableName1 = tcName1[0];
			String tableName2;
			if(tableName == null){
				tableName2 = tcName2[0];
			}else{
				tableName2 = tableName;
			}
			
			String colName1 = tcName1[1];
			String colName2 = tcName2[1];
		
			int colNum1 = tool.getColNum(schema, tableName1, colName1);
			int colNum2 = tool.getColNum(oldSchema, tableName2, colName2);
			

			if(rowString1[colNum1].equals(rowString2[colNum2])&&!(rowString1[colNum1].equalsIgnoreCase("null"))){
			
				for (int i = 0; i < rowString2.length; i++) {
					if(i!=colNum2){
						if(i == rowString2.length-1){
							row.append("|"+rowString2[i]);
							break;
						}
						
						row.append("|"+rowString2[i]);
					}
				}
				
			}else{
				row.delete(0, row.length());
			}
			
			//flag is used to check if this is the first time join two tables
			if(updatFlag){
				if(flag){
					//create schema
					createSchema(oldSchema, tableName1, tableName2, colName1, colName2);
				}else{
					createSchema1(schema,oldSchema, tableName1, tableName2, colName1, colName2);
				}
			}
			
			
		}
			
		return row;
	}
	
	//update one table schema
	public static void createSchema1(HashMap<String, HashMap<String, String>> schema,
			HashMap<String, HashMap<String, String>> oldSchema,
			String tableName1,String tableName2, String colName1, String colName2){
	
		newSchema = new HashMap<>(schema);
		
		HashMap<String, String> colHashMap1 = new HashMap<>();
		HashMap<String, String> colHashMap2 = new HashMap<>();
		
		//assign index for the second table column
		String temp = "";
		String[] str;
		Iterator it = oldSchema.get(tableName2).entrySet().iterator();
		String colName = null;
		int colNum;
		
		//find the total number of columns of first table
		int countZ = index+1;
		int joinColNum = tool.getColNum(oldSchema, tableName2, colName2);
		
				
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        if(colName2.equals((String)pair.getKey())){
	        	colNum = tool.getColNum(schema, tableName1, colName1);
	        	temp = (String) pair.getValue();
		        str = temp.split(",");
		        str[0] = String.valueOf(colNum);
		        temp = str[0]+","+str[1];
	        	colHashMap2.put(colName2, temp);
	        	continue;
	        }
	        
	        colName = (String)pair.getKey();
	        colNum = tool.getColNum(oldSchema, tableName2, colName);
	        if(joinColNum < colNum){
	        	colNum+=countZ-1;
	        }else{
	        	colNum += countZ;
	        }
	        temp = (String) pair.getValue();
	        str = temp.split(",");
	        str[0] = String.valueOf(colNum);
	        temp = str[0]+","+str[1]; 
	        colHashMap2.put(colName, temp); 
	        index++;
	    }
	    newSchema.put(tableName2, colHashMap2);
	}
	
	//update two table schema
	public static void createSchema(HashMap<String, HashMap<String, String>> oldSchema,
			String tableName1, String tableName2, String colName1, String colName2){
		
		newSchema = new HashMap<>(createTable.allTable);
	
		HashMap<String, String> colHashMap1 = new HashMap<>();
		HashMap<String, String> colHashMap2 = new HashMap<>();
		
		//find the total number of columns of first table
		int countZ = oldSchema.get(tableName1).size()-1;
		index = countZ;
		int joinColNum = tool.getColNum(oldSchema, tableName2, colName2);
		countZ++;

		//assign index for the second table column
		String temp = "";
		String[] str;
		
		Iterator it = oldSchema.get(tableName1).entrySet().iterator();
		while (it.hasNext()) {
		       Map.Entry pair = (Map.Entry)it.next();
		       colHashMap1.put((String)pair.getKey(), (String)pair.getValue());
		      
		}
		newSchema.put(tableName1, colHashMap1);
		
		it = oldSchema.get(tableName2).entrySet().iterator();
		String colName;
		int colNum;
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        if(colName2.equals((String)pair.getKey())){
	        	colNum = tool.getColNum(oldSchema, tableName1, colName1);
	        	temp = (String) pair.getValue();
		        str = temp.split(",");
		        str[0] = String.valueOf(colNum);
		        temp = str[0]+","+str[1];
	        	colHashMap2.put(colName2, temp);
	        	continue;
	        }
	        
	        colName = (String)pair.getKey();
	        colNum = tool.getColNum(oldSchema, tableName2, colName);
	        if(joinColNum < colNum){
	        	colNum+=countZ-1;
	        }else{
	        	colNum += countZ;
	        }
	        temp = (String) pair.getValue();
	        str = temp.split(",");
	        str[0] = String.valueOf(colNum);
	        temp = str[0]+","+str[1]; 
	        colHashMap2.put(colName, temp); 
	        index++;
	    }
	    newSchema.put(tableName2, colHashMap2);
	   
	}

	public static HashMap<String, HashMap<String, String>> getNewSchema() {
		return newSchema;
	}
	
	//join two tables with two conditions
	public  static StringBuilder twoJoinCond(HashMap<String, HashMap<String, String>> schema,
			HashMap<String, HashMap<String, String>> oldSchema,
			StringBuilder row1, StringBuilder row2, ArrayList<String> query,boolean updateFlag){
		StringBuilder row = new StringBuilder();
		if (row1.length() > 0 && row2.length() > 0) {

			row.append(row1.toString());
			String[] rowString1 = row.toString().split("\\|");
			String[] rowString2 = row2.toString().split("\\|");
			int colNum1;
			int colNum2;
			int colNum3;
			int colNum4;


			// parse query to get table name and column name
			if (updateFlag) {
				String[] qstr = query.get(0).split("\\s+");
				String[] tcName1 = qstr[0].split("\\.");
				String[] tcName2 = qstr[2].split("\\.");


				String tableName1 = tcName1[0];
				String tableName2=tcName2[0];

				String colName1 = tcName1[1];
				String colName2 = tcName2[1];

				colNum1 = tool.getColNum(schema, tableName1, colName1);

				colNum2 = tool.getColNum(oldSchema, tableName2, colName2);
				processPlainSelect.pos1 = colNum1;
				processPlainSelect.pos2 = colNum2;
				qstr = query.get(1).split("\\s+");
				tcName1 = qstr[0].split("\\.");
				tcName2 = qstr[2].split("\\.");

				String colName3 = tcName1[1];
				String colName4 = tcName2[1];

				colNum3 = tool.getColNum(schema, tableName1, colName3);
				colNum4 = tool.getColNum(oldSchema, tableName2, colName4);
				processPlainSelect.pos3 = colNum3;
				processPlainSelect.pos4 = colNum4;
				// create schema
				createSchema2(oldSchema, schema, tableName1, tableName2, colName2,colName4,colNum1, colNum2,colNum3,colNum4);
			}else{
				colNum1 = processPlainSelect.pos1;
				colNum2 = processPlainSelect.pos2;
				colNum3 = processPlainSelect.pos3;
				colNum4 = processPlainSelect.pos4;
			}



			if (rowString1[colNum1].equals(rowString2[colNum2])&&rowString1[colNum3].equals(rowString2[colNum4])
					&& !(rowString1[colNum1].equalsIgnoreCase("null"))&& !(rowString1[colNum3].equalsIgnoreCase("null"))) {

				for (int i = 0; i < rowString2.length; i++) {
					if (i != colNum2&&i!=colNum4) {
						row.append("|" + rowString2[i]);
					}
				}

			} else {
				row.delete(0, row.length());
			}
		}

		return row;
	}

	// update two table schema (two columns are the same)
	public static void createSchema2(
			HashMap<String, HashMap<String, String>> oldSchema,
			HashMap<String, HashMap<String, String>> schema,
			String tableName1, String tableName2,String colName2,String colName4,
			int colNum1,int colNum2,int colNum3,int colNum4) {

		newSchema = schema;

		HashMap<String, String> colHashMap1 = new HashMap<>();
		HashMap<String, String> colHashMap2 = new HashMap<>();

		// find the total number of columns of first table
		int countZ = oldSchema.get(tableName1).size() - 1;
		//  int joinColNum1 = tool.getColNum(oldSchema, tableName2, colName2);
		countZ++;

		// assign index for the second table column
		String temp = "";
		String[] str;

		Iterator it = oldSchema.get(tableName1).entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			colHashMap1.put((String) pair.getKey(), (String) pair.getValue());

		}
		newSchema.put(tableName1, colHashMap1);

		it = oldSchema.get(tableName2).entrySet().iterator();
		String colName;
		int colNum;
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			if (colName2.equals((String) pair.getKey())) {
				temp = (String) pair.getValue();
				str = temp.split(",");
				str[0] = String.valueOf(colNum1);
				temp = str[0] + "," + str[1];
				colHashMap2.put(colName2, temp);
				continue;
			}else if(colName4.equals((String) pair.getKey())){
				temp = (String) pair.getValue();
				str = temp.split(",");
				str[0] = String.valueOf(colNum3);
				temp = str[0] + "," + str[1];
				colHashMap2.put(colName4, temp);
				continue;
			}

			colName = (String) pair.getKey();
			colNum = tool.getColNum(oldSchema, tableName2, colName);
			if ((colNum2 < colNum) && (colNum < colNum4)) {
				colNum += countZ - 1;
			} else if(colNum2 < colNum && colNum4 < colNum){
				colNum += countZ -2;
			}else{
				colNum += countZ;
			}
			temp = (String) pair.getValue();
			str = temp.split(",");
			str[0] = String.valueOf(colNum);
			temp = str[0] + "," + str[1];
			colHashMap2.put(colName, temp);

		}
		newSchema.put(tableName2, colHashMap2);

	}

}
