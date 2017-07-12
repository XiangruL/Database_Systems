package sql.evaluator;

import java.util.ArrayList;


public class processPlainSelect {
	/** generate relation algebra **/
	protected static ArrayList<StringBuilder> result;
	
	public static ArrayList<StringBuilder> algebraGen(
			ArrayList<String> joinTable, ArrayList<String> whereCondition, ArrayList<String> havingCondition, 
			ArrayList<String> groupByTable, ArrayList<String> orderByTable, ArrayList<String> fromTable,
			ArrayList<String> projectTable, ArrayList<String> distinctTable, ArrayList<StringBuilder> subRes) {
		// control update schema or not
		boolean updateSchema = true;

		
		
		ArrayList<scan> scanners = new ArrayList<scan>();
		result = new ArrayList<StringBuilder>();
		
		for (String s : fromTable) {
			scan current = new scan(s);
			scanners.add(current);
		}
		parseSelect.schema = createTable.allTable;
			
		/* No join */
		if (joinTable.size() == 0) {
			for (int i = 0; i < scanners.get(0).getTotalLineNum(); i++) {
				StringBuilder temp = scanners.get(0).scanFile();
				processAfterJoin(temp, whereCondition, havingCondition, groupByTable, orderByTable, fromTable, projectTable, distinctTable, subRes);
			}
		}

		/* join */
		/* Two tables inner join */
		if (joinTable.size() == 1 && fromTable.size() == 2) {
			StringBuilder temp1;
			StringBuilder temp2;
			StringBuilder temp3;
			
			for (int i = 0; i < scanners.get(0).getTotalLineNum(); i++) {
				temp1 = scanners.get(0).scanFile();
				for (int j  = 0; j < scanners.get(1).getTotalLineNum(); j++) {
					temp3 = scanners.get(1).scanFile();
					temp2 = join.joinRow(createTable.allTable, createTable.allTable, null, temp1, temp3, joinTable.get(0), true, updateSchema);
					parseSelect.schema = join.getNewSchema();
					updateSchema = false;
					if (temp2.length() != 0) {
						processAfterJoin(temp2, whereCondition, havingCondition, groupByTable, orderByTable, fromTable, projectTable, distinctTable, subRes);
					}
				}
			}
			for (scan s : scanners) {
				s.close();
			}
		}

		/* Three tables inner join or with a cross product */
		if (joinTable.size() == 2 || (fromTable.size() == 3 && joinTable.size() == 1)) {
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
							if(joinTable.size() == 1) {
								temp3 = cross.crossProduct(parseSelect.schema, parseSelect.schema, fromTable.get(2).toString(), temp2, scanners.get(2).scanFile(), updateSchema2);
							} else {
								temp3 = join.joinRow(parseSelect.schema, createTable.allTable, fromTable.get(2), temp2, scanners.get(2).scanFile(), joinTable.get(1), false, updateSchema2);
								parseSelect.schema = join.getNewSchema();
							}
							updateSchema2 = false;
						}
						if (temp3.length() != 0) {
							processAfterJoin(temp3, whereCondition, havingCondition, groupByTable, orderByTable, fromTable, projectTable, distinctTable, subRes);
						}
					}				
				}
			}
			for (scan s : scanners) {
				s.close();
			}
		}

		
		/* Four tables inner join or with a cross product */
		if (joinTable.size() == 3 || (fromTable.size() == 4 && joinTable.size() == 2)) {
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
								if(joinTable.size() == 2) {
//									temp4 = cross.crossProduct(parseSelect.schema, parseSelect.schema, fromTable.get(3).toString(), temp3, scanners.get(3).scanFile(), updateSchema3);
								} else {
									temp4 = join.joinRow(parseSelect.schema, createTable.allTable, fromTable.get(3), temp3, scanners.get(3).scanFile(), joinTable.get(2), false, updateSchema3);
									parseSelect.schema = join.getNewSchema();
								}
								updateSchema3 = false;
							}
							if (temp4.length() != 0) {
								processAfterJoin(temp4, whereCondition, havingCondition, groupByTable, orderByTable, fromTable, projectTable, distinctTable, subRes);
							}
						}
					}				
				}
			}
			for (scan s : scanners) {
				s.close();
			}
		}

		// if there some operation such as having to handle a whole table, pass it to this method
		processWholeTable(havingCondition, groupByTable, orderByTable, fromTable, projectTable, distinctTable, subRes);
			
		return result;
	}
		
	
	public static void processAfterJoin(StringBuilder temp, ArrayList<String> whereCondition, ArrayList<String> havingCondition, 
			ArrayList<String> groupByTable, ArrayList<String> orderByTable, ArrayList<String> fromTable,
			ArrayList<String> projectTable, ArrayList<String> distinctTable, ArrayList<StringBuilder> subRes) {
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
					temp2 = select.InSelect(parseSelect.schema, temp, s, subRes, fromTable);
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
				projection.proTable(result, parseSelect.schema, temp2, query, true, fromTable);
			} else {
				projection.proTable(result, parseSelect.schema, temp2, query, false, fromTable);
			}
		}
	}
	
	public static void processWholeTable(ArrayList<String> havingCondition, 
			ArrayList<String> groupByTable, ArrayList<String> orderByTable, ArrayList<String> fromTable,
			ArrayList<String> projectTable, ArrayList<String> distinctTable, ArrayList<StringBuilder> subRes) {

		/* having */
		if (!havingCondition.isEmpty()) {
			// havingSelect(HashMap<String, HashMap<String, String>> schema, ArrayList<StringBuilder> result, String aggQuery, String groupQuery)
			having.havingSelect(parseSelect.schema, result, havingCondition.get(0).toString().toLowerCase(), groupByTable.get(0), fromTable);
		}
		
		/* order by */
		if (!havingCondition.isEmpty() && orderByTable != null && orderByTable.get(0) != null) {
			// havingSelect(HashMap<String, HashMap<String, String>> schema, ArrayList<StringBuilder> result, String aggQuery, String groupQuery)
			result = order.orderBy(parseSelect.schema, result, orderByTable.get(0), fromTable);
		}
		
		/* projection if there has group by */
		if (groupByTable.get(0) != null) {

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
