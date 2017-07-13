package sql.evaluator;

import java.util.ArrayList;


public class processPlainSelect {
	/** generate relation algebra **/
	protected static ArrayList<StringBuilder> result;
	
	public static ArrayList<StringBuilder> algebraGen(parseSelect p) {
		// control update schema or not
		boolean updateSchema = true;

		
		ArrayList<scan> scanners = new ArrayList<scan>();
		result = new ArrayList<StringBuilder>();
		
		for (String s : p.fromTable) {
			scan current = new scan(s);
			scanners.add(current);
		}
		p.schema = createTable.allTable;
			
		/* No join */
		if (p.joinTable.size() == 0) {
			for (int i = 0; i < scanners.get(0).getTotalLineNum(); i++) {
				StringBuilder temp = scanners.get(0).scanFile();
				processAfterJoin(p, temp);
			}
		}

		/* join */
		/* Two tables inner join */
		if (p.joinTable.size() == 1 && p.fromTable.size() == 2) {
			StringBuilder temp1;
			StringBuilder temp2;
			StringBuilder temp3;
			
			for (int i = 0; i < scanners.get(0).getTotalLineNum(); i++) {
				temp1 = scanners.get(0).scanFile();
				for (int j  = 0; j < scanners.get(1).getTotalLineNum(); j++) {
					temp3 = scanners.get(1).scanFile();
//					System.out.println(temp3);

					temp2 = join.joinRow(createTable.allTable, createTable.allTable, null, temp1, temp3, p.joinTable.get(0), true, updateSchema);

					p.schema = join.getNewSchema();
					updateSchema = false;
					if (temp2.length() != 0) {
						processAfterJoin(p, temp2);
					}
				}
			}
			for (scan s : scanners) {
				s.close();
			}
		}

		/* Three tables inner join or with a cross product */
		if (p.joinTable.size() == 2 || (p.fromTable.size() == 3 && p.joinTable.size() == 1)) {
			StringBuilder temp1;
			StringBuilder temp2;
			StringBuilder temp3 = new StringBuilder();
			boolean updateSchema1 = true;
			boolean updateSchema2 = true;
			for (int i = 0; i < scanners.get(0).getTotalLineNum(); i++) {
				temp1 = scanners.get(0).scanFile();
				for (int j  = 0; j < scanners.get(1).getTotalLineNum(); j++) {
					temp2 = new StringBuilder();
					
					temp2 = join.joinRow(createTable.allTable, createTable.allTable, null, temp1, scanners.get(1).scanFile(), p.joinTable.get(0), true, updateSchema1);
					p.schema = join.getNewSchema();
					updateSchema1 = false;
					for (int k  = 0; k < scanners.get(2).getTotalLineNum(); k++) {
						temp3 = new StringBuilder();
						if (temp2.length() != 0) {
							if(p.joinTable.size() == 1) {
								temp3 = cross.crossProduct(p.schema, p.schema, p.fromTable.get(2).toString(), temp2, scanners.get(2).scanFile(), updateSchema2);
							} else {
								temp3 = join.joinRow(p.schema, createTable.allTable, p.fromTable.get(2), temp2, scanners.get(2).scanFile(), p.joinTable.get(1), false, updateSchema2);
								p.schema = join.getNewSchema();
							}
							updateSchema2 = false;
						}
						if (temp3.length() != 0) {
							processAfterJoin(p, temp3);
						}
					}				
				}
			}
			for (scan s : scanners) {
				s.close();
			}
		}

		
		/* Four tables inner join or with a cross product */
		if (p.joinTable.size() == 3 || (p.fromTable.size() == 4 && p.joinTable.size() == 2)) {
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
					temp2 = join.joinRow(createTable.allTable, createTable.allTable, null, temp1, scanners.get(1).scanFile(), p.joinTable.get(0), true, updateSchema1);
					p.schema = join.getNewSchema();
					updateSchema1 = false;
					for (int k  = 0; k < scanners.get(2).getTotalLineNum(); k++) {
						temp3 = new StringBuilder();
						if (temp2.length() != 0) {
							temp3 = join.joinRow(p.schema, createTable.allTable, p.fromTable.get(2), temp2, scanners.get(2).scanFile(), p.joinTable.get(1), false, updateSchema2);
							p.schema = join.getNewSchema();
							updateSchema2 = false;
						}
						for (int l  = 0; l < scanners.get(3).getTotalLineNum(); l++) {
							temp4 = new StringBuilder();
							if (temp3.length() != 0) {
								if(p.joinTable.size() == 2) {
//									temp4 = cross.crossProduct(parseSelect.schema, parseSelect.schema, fromTable.get(3).toString(), temp3, scanners.get(3).scanFile(), updateSchema3);
								} else {
									temp4 = join.joinRow(p.schema, createTable.allTable, p.fromTable.get(3), temp3, scanners.get(3).scanFile(), p.joinTable.get(2), false, updateSchema3);
									p.schema = join.getNewSchema();
								}
								updateSchema3 = false;
							}
							if (temp4.length() != 0) {
								processAfterJoin(p, temp4);
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
		processWholeTable(p);
			
		return result;
	}
		
	
	public static void processAfterJoin(parseSelect p, StringBuilder temp) {
		StringBuilder temp2 = temp;
		boolean groupByFlag = false;
		boolean orderByFlag = false;
		String query = "";
		/* where condition */
		if (p.whereCondition.size() != 0) {
			for (String s : p.whereCondition) {
				String[] whereS = s.split(" ");
				if (whereS.length == 2 && whereS[1].equals("in")) {
					// InSelect(HashMap<String, HashMap<String, String>> schema, StringBuilder row, String query, ArrayList<StringBuilder> result)
					temp2 = select.InSelect(p.schema, temp, s, p.subRes, p.fromTable);
				} else {
					temp2 = select.selectRow(p.schema, temp, s, p.fromTable);
				}
				// not meet the select requirement
				if (temp2 == null || temp2.length() == 0) {
					break;
				}
			}
		}	

		

		/* group by */
		if (p.groupByTable != null && p.groupByTable.get(0) != null && temp2.length() != 0) {
			groupByFlag = true;
			result = group.groupBy(result, p.schema, temp2, p.groupByTable.get(0), p.fromTable);

		}
		

		/* order by */
		if (p.orderByTable != null && p.orderByTable.get(0) != null) {
			if (groupByFlag) {
				// orderBy2(ArrayList<StringBuilder> result, HashMap<String, HashMap<String, String>> scheme,String query,StringBuilder singleRecord )
				result = order.orderBy(p.schema, result, temp2, p.orderByTable.get(0), p.groupByTable.get(0), p.fromTable);
			} else {
				// orderBy(HashMap<String, HashMap<String, String>> schema, ArrayList<StringBuilder> result, String orderQuery,StringBuilder row )
				
				result = order.orderBy(p.schema, result, temp2, p.orderByTable.get(0), p.groupByTable.get(0), p.fromTable);
			}		
		}
		
		/* projection if there is no group by */
		if (!groupByFlag) {
			query = p.projectTable.toString().substring(1, p.projectTable.toString().length() - 1);
			if (p.distinctTable != null) {
				projection.proTable(result, p.schema, temp2, query, true, p.fromTable);
			} else {
				projection.proTable(result, p.schema, temp2, query, false, p.fromTable);
			}
		}
	}
	
	public static void processWholeTable(parseSelect p) {

		/* having */
		if (!p.havingCondition.isEmpty()) {
			// havingSelect(HashMap<String, HashMap<String, String>> schema, ArrayList<StringBuilder> result, String aggQuery, String groupQuery)
			having.havingSelect(p.schema, result, p.havingCondition.get(0).toString().toLowerCase(), p.groupByTable.get(0), p.fromTable);
		}
		
		/* order by */
		if (!p.havingCondition.isEmpty() && p.orderByTable != null && p.orderByTable.get(0) != null) {
			// havingSelect(HashMap<String, HashMap<String, String>> schema, ArrayList<StringBuilder> result, String aggQuery, String groupQuery)
			result = order.orderBy(p.schema, result, p.orderByTable.get(0), p.fromTable);
		}
		
		/* projection if there has group by */
		if (p.groupByTable.get(0) != null) {

			String query;
			query = p.projectTable.toString().substring(1, p.projectTable.toString().length() - 1);

			if (p.distinctTable != null) {
				result = projection.proTable(result, p.schema, query, true, p.fromTable);
			} else {
				result = projection.proTable(result, p.schema, query, false, p.fromTable);
				
			}
		}
	
	}
}
