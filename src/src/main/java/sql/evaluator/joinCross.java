package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.statement.select.SelectBody;

public class joinCross {
	
	public static ArrayList<StringBuilder> jc(HashMap<String, HashMap<String, String>> oldSchema,
			HashMap<String, HashMap<String, String>> newSchema, String joinQuery, String crossTableName,
			ArrayList<StringBuilder> result,
			ArrayList<String> joinTable, ArrayList<String> whereCondition, ArrayList<String> havingCondition, 
			ArrayList<String> groupByTable, ArrayList<String> orderByTable, ArrayList<String> fromTable,
			ArrayList<String> projectTable, ArrayList<String> distinctTable, ArrayList<SelectBody> subQueryTable){
		//parse query to get table name
	
		String[] qstr = joinQuery.split("\\s+");
		String[] tcName1 = qstr[0].split("\\.");
		String[] tcName2 = qstr[2].split("\\.");
		
		String tableName1 = tcName1[0];
		String tableName2 = tcName2[0];
	
		scan scan1 = new scan(tableName1);
		scan scan2 = new scan(tableName2);
		scan scan3 = new scan(crossTableName);
		
		StringBuilder row= new StringBuilder();
		StringBuilder row1= new StringBuilder();
		StringBuilder row2= new StringBuilder();
		StringBuilder row3= new StringBuilder();
		StringBuilder row4= new StringBuilder();
		
		int count = 0;
		boolean updateFlag1 = true;
		boolean updateFlag2 = true;
		//The first file
		while(!scan1.isFlag()){
			row1 = scan1.scanFile();
			
			if(scan2.isFlag()){
				scan2.setFlag(false);
			}
			//The second file
			while(!scan2.isFlag()){
				row2 = scan2.scanFile();
				
				if(scan3.isFlag()){
					scan3.setFlag(false);
				}
				row3 = join.joinRow(newSchema,oldSchema,null,row1, row2,joinQuery, true,updateFlag1);
							
				if(count == 0){
					newSchema = join.getNewSchema();
				
					updateFlag1 = false;
					count++;
					
				}
				//the third file(cross product)
				while(!scan3.isFlag()){
					row4 = scan3.scanFile();
					row = cross.crossProduct(oldSchema, newSchema, crossTableName, row3, row4, joinQuery, updateFlag2);
					if(count == 1){
						updateFlag2 = false;
						count++;
					}
					//if row exist, do the next step
					if(row.length()>0){
						processPlainSelect.processAfterJoin(row, whereCondition, havingCondition, groupByTable, orderByTable, fromTable, projectTable, distinctTable, subQueryTable);
					}
				}	
			}
		}
		return result;
	}
	
	
	
	
}