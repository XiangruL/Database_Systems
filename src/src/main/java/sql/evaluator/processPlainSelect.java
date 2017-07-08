package sql.evaluator;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.statement.select.SelectBody;

public class processPlainSelect {
	/** generate relation algebra **/
	protected static ArrayList<StringBuilder> result;
	
	public static ArrayList<StringBuilder> algebraGen(
			ArrayList<String> joinTable, ArrayList<String> whereCondition, ArrayList<String> havingCondition, 
			ArrayList<String> groupByTable, ArrayList<String> orderByTable, ArrayList<String> fromTable,
			ArrayList<String> projectTable, ArrayList<String> distinctTable, ArrayList<SelectBody> subQueryTable) {
		// control it's the first time to enter the join loop to scan a line
		boolean startFlag = true;
		// control to stop the outer loop or not
		boolean stopFlag = true;
		// control update schema or not
		boolean updateSchema = true;
		// order by exists or not
		boolean finishFlag = false;
		
		
		ArrayList<scan> scanners = new ArrayList<scan>();
		result = new ArrayList<StringBuilder>();
		
		for (String s : fromTable) {
			scan current = new scan(s);
			scanners.add(current);
		}
		stopFlag = scanners.get(0).isFlag();
		boolean[] flagSet = new boolean[fromTable.size() - 1];
		StringBuilder[] rowSet = new StringBuilder[fromTable.size() - 1];
		HashMap[] schemaSet = new HashMap[fromTable.size() - 1];
		parseSelect.schema = createTable.allTable;
		
//		while (!stopFlag) {
//			parseSelect.schema = createTable.allTable;
//			stopFlag = scanners.get(0).isFlag();
//			scanners.get(0).isFlag();
//			StringBuilder temp2 = new StringBuilder();
//			int joinIndex = 0;
			
		if (joinTable.size() == 0) {
			for (int i = 0; i < scanners.get(0).getTotalLineNum(); i++) {
				StringBuilder temp = scanners.get(0).scanFile();
				processAfterJoin(temp, whereCondition, havingCondition, groupByTable, orderByTable, fromTable, projectTable, distinctTable, subQueryTable);
			}
		}

		/* join */
		if (joinTable.size() == 1) {
			StringBuilder temp1;
			StringBuilder temp2;
			StringBuilder temp3;
			for (int i = 0; i < scanners.get(0).getTotalLineNum(); i++) {
				temp1 = scanners.get(0).scanFile();
				for (int j  = 0; j < scanners.get(1).getTotalLineNum(); j++) {
					temp3 = scanners.get(1).scanFile();
//					System.out.println(temp3);
					temp2 = join.joinRow(createTable.allTable, createTable.allTable, null, temp1, temp3, joinTable.get(0), true, updateSchema);
					parseSelect.schema = join.getNewSchema();
					updateSchema = false;
					if (temp2.length() != 0) {
//						System.out.println(parseSelect.schema);
						processAfterJoin(temp2, whereCondition, havingCondition, groupByTable, orderByTable, fromTable, projectTable, distinctTable, subQueryTable);
					}
				}
			}
		}

		if (joinTable.size() == 2) {
			StringBuilder temp1;
			StringBuilder temp2;
			StringBuilder temp3 = new StringBuilder();
			boolean updateSchema1 = true;
			boolean updateSchema2 = true;
			for (int i = 0; i < scanners.get(0).getTotalLineNum(); i++) {
				temp1 = scanners.get(0).scanFile();
				for (int j  = 0; j < scanners.get(1).getTotalLineNum(); j++) {
					temp2 = new StringBuilder();
					temp2 = join.joinRow(createTable.allTable, createTable.allTable, null, temp1, scanners.get(1).scanFile(), joinTable.get(0), true, updateSchema1);
					parseSelect.schema = join.getNewSchema();
					updateSchema1 = false;
					for (int k  = 0; k < scanners.get(2).getTotalLineNum(); k++) {
						temp3 = new StringBuilder();
						if (temp2.length() != 0) {
							temp3 = join.joinRow(parseSelect.schema, createTable.allTable, fromTable.get(2), temp2, scanners.get(2).scanFile(), joinTable.get(1), false, updateSchema2);
//							System.out.println(temp3);
							parseSelect.schema = join.getNewSchema();
							updateSchema2 = false;
						}
						if (temp3.length() != 0) {
//							System.out.println(parseSelect.schema);
							processAfterJoin(temp3, whereCondition, havingCondition, groupByTable, orderByTable, fromTable, projectTable, distinctTable, subQueryTable);
						}
					}				
				}
			}
		}

		if (joinTable.size() == 3) {
			StringBuilder temp1;
			StringBuilder temp2;
			StringBuilder temp3 = new StringBuilder();
			StringBuilder temp4 = new StringBuilder();
			boolean updateSchema1 = true;
			boolean updateSchema2 = true;
			boolean updateSchema3 = true;
			for (int i = 0; i < scanners.get(0).getTotalLineNum(); i++) {
				temp1 = scanners.get(0).scanFile();
				for (int j  = 0; j < scanners.get(1).getTotalLineNum(); j++) {
					temp2 = join.joinRow(createTable.allTable, createTable.allTable, null, temp1, scanners.get(1).scanFile(), joinTable.get(0), true, updateSchema1);
					parseSelect.schema = join.getNewSchema();
					updateSchema1 = false;
					for (int k  = 0; k < scanners.get(2).getTotalLineNum(); k++) {
						temp3 = new StringBuilder();
						if (temp2.length() != 0) {
							temp3 = join.joinRow(parseSelect.schema, createTable.allTable, fromTable.get(2), temp2, scanners.get(2).scanFile(), joinTable.get(1), false, updateSchema2);
							parseSelect.schema = join.getNewSchema();
							updateSchema2 = false;
						}
						for (int l  = 0; l < scanners.get(3).getTotalLineNum(); l++) {
							temp4 = new StringBuilder();
							if (temp3.length() != 0) {
								temp4 = join.joinRow(parseSelect.schema, createTable.allTable, fromTable.get(3), temp3, scanners.get(3).scanFile(), joinTable.get(2), false, updateSchema3);
								parseSelect.schema = join.getNewSchema();
								updateSchema3 = false;
							}
							if (temp4.length() != 0) {
								processAfterJoin(temp4, whereCondition, havingCondition, groupByTable, orderByTable, fromTable, projectTable, distinctTable, subQueryTable);
							}
						}
					}				
				}
			}
		}
		processWholeTable(havingCondition, groupByTable, orderByTable, fromTable, projectTable, distinctTable, subQueryTable);
			
			
//			if (joinTable != null && !joinTable.isEmpty()) {
//				if (!flagSet[0]) {
//					temp2 = parseSelect.joinTwoTable(scanners.get(0), scanners.get(1), joinTable.get(0), startFlag, createTable.allTable, true, updateSchema);
//					schemaSet[0] = parseSelect.schema;
//					rowSet[0] = temp2;
//					startFlag = false;
//				}
//
//				for (int i = 2; i < scanners.size(); i++) {
//					if (!flagSet[i - 1]) {
//						// last join lock
//						if (!flagSet[i - 2]) {
//							if (schemaSet[i - 1] == null) {
//								schemaSet[i - 1] = parseSelect.schema;
//							}
//							temp2 = parseSelect.joinRowAndTable(rowSet[i - 2], scanners.get(i), fromTable.get(i), joinTable.get(i - 1), schemaSet[i - 2], false, updateSchema);
//						} else {
//							if (schemaSet[i - 1] == null) {
//								schemaSet[i - 1] = parseSelect.schema;
//							}
//							temp2 = parseSelect.joinRowAndTable(temp2, scanners.get(i), fromTable.get(i), joinTable.get(i - 1), schemaSet[i - 2], false, updateSchema);
//						} 
//						if (temp2.length() == 0) {
//							flagSet[i - 2] = false;
//							scanners.get(i).setFlag(false);
//							break;
//						} else {
//							if (i == scanners.size() - 1)
//								finishFlag = true;
//							rowSet[i - 1] = temp2;
//							if (scanners.get(i).isFlag()) {
//								if (i == scanners.size() - 1) {
//									flagSet[i - 2] = false;
//									scanners.get(i).setFlag(false);
//								}
//								else if (scanners.get(i + 1).isFlag()) {
//									flagSet[i - 2] = false;
//									scanners.get(i).setFlag(false);
//								}
//							} else {
//								flagSet[i - 2] = true;
//							}
//						}
//					}
//				}
				
//			} else {
//				temp2 = scanners.get(0).scanFile();
//			}
//			// after joining once, no need to update schema
//			if (finishFlag) {
////				System.out.println(temp2);
//				processAfterJoin(temp2, whereCondition, havingCondition, groupByTable, orderByTable, fromTable, projectTable, distinctTable, subQueryTable);
//			}
//			
//			stopFlag = scanners.get(0).isFlag();
//			updateSchema = false;
//		}
		
		return result;
	}
		
	
	public static void processAfterJoin(StringBuilder temp, ArrayList<String> whereCondition, ArrayList<String> havingCondition, 
			ArrayList<String> groupByTable, ArrayList<String> orderByTable, ArrayList<String> fromTable,
			ArrayList<String> projectTable, ArrayList<String> distinctTable, ArrayList<SelectBody> subQueryTable) {
		StringBuilder temp2 = temp;
		boolean groupByFlag = false;
		boolean orderByFlag = false;
		String query = "";


		/* where condition */
		if (whereCondition.size() != 0) {
			for (String s : whereCondition) {
				String[] whereS = s.split(" ");
				if (whereS.length == 2 && whereS[1].equals("in")) {
					// InSelect(HashMap<String, HashMap<String, String>> schema, StringBuilder row, String query, ArrayList<StringBuilder> result)
					temp2 = select.InSelect(parseSelect.schema, temp, s, parseSelect.subRes, fromTable);
				} else {
					temp2 = select.selectRow(parseSelect.schema, temp, s, fromTable);
				}
				// not meet the select requirement
				if (temp2 == null || temp2.length() == 0) {
					break;
				}
			}
		}	


		/* group by */
		if (groupByTable != null && groupByTable.get(0) != null && temp2.length() != 0) {
			groupByFlag = true;
			result = group.groupBy(result, parseSelect.schema, temp2, groupByTable.get(0), fromTable);
		}

		/* order by */
		if (orderByTable != null && orderByTable.get(0) != null) {
			if (groupByFlag) {
				// orderBy2(ArrayList<StringBuilder> result, HashMap<String, HashMap<String, String>> scheme,String query,StringBuilder singleRecord )
				result = order.orderBy(parseSelect.schema, result, temp2, orderByTable.get(0), groupByTable.get(0), fromTable);

			} else {
				// orderBy(HashMap<String, HashMap<String, String>> schema, ArrayList<StringBuilder> result, String orderQuery,StringBuilder row )
				result = order.orderBy(parseSelect.schema, result, temp2, orderByTable.get(0), groupByTable.get(0), fromTable);
			}		
		}
		
		/* projection if there is no group by */
		if (!groupByFlag) {
			query = projectTable.toString().substring(1, projectTable.toString().length() - 1);
			if (distinctTable != null) {
				result = projection.proTable(result, parseSelect.schema, temp2, query, true, fromTable);
			} else {
				result = projection.proTable(result, parseSelect.schema, temp2, query, false, fromTable);
			}
		}
	}
	
	public static void processWholeTable(ArrayList<String> havingCondition, 
			ArrayList<String> groupByTable, ArrayList<String> orderByTable, ArrayList<String> fromTable,
			ArrayList<String> projectTable, ArrayList<String> distinctTable, ArrayList<SelectBody> subQueryTable) {

		/* having */
		if (!havingCondition.isEmpty()) {

			// havingSelect(HashMap<String, HashMap<String, String>> schema, ArrayList<StringBuilder> result, String aggQuery, String groupQuery)
			having.havingSelect(parseSelect.schema, result, havingCondition.get(0).toString().toLowerCase(), groupByTable.get(0), fromTable);
		}
		
		/* orderby */
		if (orderByTable != null && orderByTable.get(0) != null) {
			// havingSelect(HashMap<String, HashMap<String, String>> schema, ArrayList<StringBuilder> result, String aggQuery, String groupQuery)
			result = order.orderBy(parseSelect.schema, result, orderByTable.get(0), fromTable);
		}
		
		/* projection if there has group by */
		if (groupByTable.get(0).length() != 0) {
			String query;
			query = projectTable.toString().substring(1, projectTable.toString().length() - 1);
			if (distinctTable != null) {
				result = projection.proTable(result, parseSelect.schema, query, true, fromTable);
			} else {
				result = projection.proTable(result, parseSelect.schema, query, false, fromTable);
			}
		}
	
	}
}
