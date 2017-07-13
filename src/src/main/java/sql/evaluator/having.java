package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;

public class having {

	public static ArrayList<StringBuilder> havingSelect(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result, String aggQuery, String groupQuery,ArrayList<String> tableNames){
		//Ex. query = "count(*) > 1"
		
		//get the type and column of attribute
		String[] qstr = aggQuery.split("\\s+");
		
		aggregation.aggRow(schema, result, qstr[0], groupQuery,tableNames);
		
		String tableName = "agg";
		String colName = qstr[0].split("\\(")[0];
		
		int colNum = tool.getColNum(schema, tableName, colName);
		double operand = Double.valueOf(qstr[2]);
		String operator = qstr[1];
		
		String[] rowStrings=null;
		double data = 0.0;

		for (int i = 0; i < result.size(); i++) {
			rowStrings = result.get(i).toString().split("\\|");
			data = Double.valueOf(rowStrings[colNum]);
			
			switch (operator) {
			case ">":
				if(data <= operand){
					result.remove(i);
					i--;
				}
				break;
			case "<":
				if(data >= operand){
					result.remove(i);
					i--;
				}
				break;
				
			case "=":
				if(!(data == operand)){
					result.remove(i);
					i--;
				}
				break;
				
			case ">=":
				if(data < operand){
					result.remove(i);
					i--;
				}
				break;
				
			case "<=":
				if(data > operand){
					result.remove(i);
					i--;
				}
				break;
			
			case "!=":
				if(data != operand){
					result.remove(i);
					i--;
				}
				break;

			default:
				break;
			}
		}
		
		return result;
	}
}
