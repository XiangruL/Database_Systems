package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.statement.select.SelectBody;

public class joinCross {
	
	public static ArrayList<StringBuilder> jcThree(HashMap<String, HashMap<String, String>> oldSchema,
			HashMap<String, HashMap<String, String>> newSchema, ArrayList<String> joinQuery, String crossTableName,
			ArrayList<StringBuilder> result,
			ArrayList<String> joinTable, ArrayList<String> whereCondition, ArrayList<String> havingCondition, 
			ArrayList<String> groupByTable, ArrayList<String> orderByTable, ArrayList<String> fromTable,
			ArrayList<String> projectTable, ArrayList<String> distinctTable, ArrayList<SelectBody> subQueryTable){
		//parse query to get table name
		String[] qstr = joinQuery.get(0).split("\\s+");
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
				row3 = join.joinRow(newSchema,oldSchema,null,row1, row2,joinQuery.get(0), true,updateFlag1);
							
				if(count == 0){
					newSchema = join.getNewSchema();
					updateFlag1 = false;
					count++;
					
				}
				//the third file(cross product)
				while(!scan3.isFlag()){
					row4 = scan3.scanFile();
					row = cross.crossProduct(oldSchema, newSchema, crossTableName, row3, row4, joinQuery.get(0), updateFlag2);
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
	
	public static ArrayList<StringBuilder> jcFour(HashMap<String, HashMap<String, String>> oldSchema,
			HashMap<String, HashMap<String, String>> newSchema, ArrayList<String> joinQuery, String crossTableName,
			ArrayList<StringBuilder> result,
			ArrayList<String> joinTable, ArrayList<String> whereCondition, ArrayList<String> havingCondition, 
			ArrayList<String> groupByTable, ArrayList<String> orderByTable, ArrayList<String> fromTable,
			ArrayList<String> projectTable, ArrayList<String> distinctTable, ArrayList<SelectBody> subQueryTable){
		//parse query to get table name
	
		String[] qstr = joinQuery.get(0).split("\\s+");
		String[] tcName1 = qstr[0].split("\\.");
		String[] tcName2 = qstr[2].split("\\.");
		
		String tableName1 = tcName1[0];
		String tableName2 = tcName2[0];
	
		qstr = joinQuery.get(1).split("\\s+");
		String[] tcName3 = qstr[2].split("\\.");
		String tableName3 = tcName3[0];
		
		scan scan1 = new scan(tableName1);
		scan scan2 = new scan(tableName2);
		scan scan3 = new scan(tableName3);
		scan scan4 = new scan(crossTableName);
		
		StringBuilder row= new StringBuilder();
		StringBuilder row1= new StringBuilder();
		StringBuilder row2= new StringBuilder();
		StringBuilder row3= new StringBuilder();
		StringBuilder row4= new StringBuilder();
		StringBuilder row5= new StringBuilder();
		StringBuilder row6= new StringBuilder();
		
		int count = 0;
		int temp1 = 0;
		int temp2 = 0;
		int temp3 = 0;
		int temp4 = 0;
		boolean updateFlag1 = true;
		boolean updateFlag2 = true;
		boolean updateFlag3 = true;
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
				row3 = join.joinRow(newSchema,oldSchema,null,row1, row2,joinQuery.get(0), true,updateFlag1);
						
				if(count == 0){
					newSchema = join.getNewSchema();
					updateFlag1 = false;
					count++;
					
				}
				//the third file
				while(!scan3.isFlag()){
					row4 = scan3.scanFile();
					if(scan4.isFlag()){
						scan4.setFlag(false);
					}
					row5 = join.joinRow(newSchema,oldSchema,null,row3, row4,joinQuery.get(1), false,updateFlag2);
					
					if(count == 1){
						newSchema = join.getNewSchema();
						updateFlag2 = false;
						count++;
					}
					//the fourth file(cross product)
					while(!scan4.isFlag()){
						row6 = scan4.scanFile();
						row = cross.crossProduct(oldSchema, newSchema, crossTableName, row5, row6, joinQuery.get(1), updateFlag3);
						if(count == 2){
							updateFlag3 = false;
							count++;
						}
						
						//if row exist, do the next step
						if(row.length()>0){
							System.out.println(row);
							//processPlainSelect.processAfterJoin(row, whereCondition, havingCondition, groupByTable, orderByTable, fromTable, projectTable, distinctTable, subQueryTable);
						}
					}
					
				}	
			}
		}
		return result;
	}
	
	
}