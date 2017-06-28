package sql.evaluator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class join {
	public StringBuilder joinRow(HashMap<String, HashMap<String, String>> scheme,
			StringBuilder row1, StringBuilder row2,
			String colName1, String colName2){
		
		String[] rowString1 = row1.toString().split("\\|");
		String[] rowString2 = row2.toString().split("\\|");
		
		String tableName1 = rowString1[rowString1.length-1];
		String tableName2 = rowString2[rowString2.length-1];
		
		int colNum1 = tool.getColNum(scheme, tableName1, colName1);
		int colNum2 = tool.getColNum(scheme, tableName2, colName2);
		
		if(rowString1[colNum1].equals(rowString2[colNum2])){
			//create scheme
			HashMap<String, String> newScheme = createScheme(scheme,tableName1,tableName2,colName2);
			scheme.put("newtable", newScheme);
			
			row1.delete(row1.length()-1-tableName1.length(), row1.length());
			
			for (int i = 0; i < rowString2.length; i++) {
				if(!rowString2[i].equals(colName2)){
					if(i == rowString2.length-1){
						row1.append("newtable");
						break;
					}
					row1.append(rowString2[i]+"|");
				}
			}
			
		}else{
			row1.delete(0, row1.length());
		}
		
		
		
		return row1;
	}
	
	public HashMap<String, String> createScheme(HashMap<String, HashMap<String, String>> scheme,
			String tableName1, String tableName2, String columnName){
		
		HashMap<String, String> newScheme = new HashMap<String, String>();
		
		
		//find the total number of columns of first table
		int count = scheme.get(tableName1).size();
		
		//assign index for the second table column
		String temp = "";
		String[] str;
		
		Iterator it = scheme.get(tableName1).entrySet().iterator();
		while (it.hasNext()) {
		       Map.Entry pair = (Map.Entry)it.next();
		       newScheme.put((String)pair.getKey(), (String)pair.getValue());
		       
		}
		
		it = scheme.get(tableName2).entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        if(columnName.equals((String)pair.getKey())){
	        	continue;
	        }
	        temp = (String) pair.getValue();
	        str = temp.split(",");
	        str[0] = String.valueOf(count);
	        temp = str[0]+","+str[1];
	        count++;
	        System.out.println(count);
	        newScheme.put((String)pair.getKey(), temp);
	        
	    }
	   
		return newScheme;
	}
}
