package sql.evaluator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class tool {
	
	public static int getColNum(HashMap<String, HashMap<String, String>> schema,
			String tableName, String colName){
		//get the column information
		HashMap<String, String> tableMap1 = schema.get(tableName);
		int colNum = -1;
	
		//check if table contain this colName
		//get the column number
		if(tableMap1.containsKey(colName)){
			String col1 = tableMap1.get(colName);
					
			//split the column number and type into an array
			String[] str1 = col1.split(",");
							
			//get the column number, cast to Integer;
			colNum = Integer.valueOf(str1[0]);
		}
		return colNum;
	}
	
	public static String getColType(HashMap<String, HashMap<String, String>> schema,
			String tableName, String colName){
		//get the column information
		HashMap<String, String> tableMap1 = schema.get(tableName);
		String colType = "-";
				
		//check if table contain this colName
		//get the column number and type
		if(tableMap1.containsKey(colName)){
			String col1 = tableMap1.get(colName);
			//split the column number and type into an array
			String[] str1 = col1.split(",");
							
			//get the type;
			colType = str1[1];
		}
		return colType;
	}
	
	//check operand1 is larger than operand2
	public static boolean isLarge(String type, String operand1, String operand2){
		
		//classify based on data type
		switch (type) {
		case "date":
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			
			try {
				Date date1 = sdf.parse(operand1);
				Date date2 = sdf.parse(operand2);
			
				if (date1.after(date2)) {
					return true;
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
			
		case "varchar":
			if(operand1.compareTo(operand2)>0){
				return true;
			}
			break;
		
		case "integer":
			int num1 = Integer.valueOf(operand1);
			int num2 = Integer.valueOf(operand2);
			if(num1 > num2){
				return true;
			}
		
			break;
			
		case "double":
			double num3 = Double.valueOf(operand1);
			double num4 = Double.valueOf(operand2);
			if(num3 > num4){
				return true;
			}
			
			break;

		default:
			break;
		}
		
		
		return false;
	}
	
	//check operand1 is small than operand2
	public static boolean isSmall(String type, String operand1, String operand2){
		return isLarge(type, operand2, operand1);
	}

}
