package sql.evaluator;

import java.util.ArrayList;

public class processPlainSelect {
	/** generate relation algebra **/
	public static ArrayList<StringBuilder> algebraGen() {
		// control it's the first time to enter the join loop to scan a line
		boolean startFlag = true;
		// control to stop the outer loop or not
		boolean stopFlag = true;
		// control update schema or not
		boolean updateSchema = true;
		// order by exists or not
		boolean groupByFlag = false;
		ArrayList<scan> scanners = new ArrayList<scan>();
		ArrayList<StringBuilder> result = new ArrayList<StringBuilder>();
		String query = "";
		for (String s : parseSelect.fromTable) {
			scan current = new scan(s);
			scanners.add(current);
		}
		stopFlag = scanners.get(0).isFlag();
		
		while (!stopFlag) {
			parseSelect.schema = createTable.allTable;
			StringBuilder temp2 = new StringBuilder();
			int joinIndex = 0;
			
			/* join */
			if (parseSelect.joinTable != null && !parseSelect.joinTable.isEmpty()) {
				temp2 = parseSelect.joinTwoTable(scanners.get(0), scanners.get(1), parseSelect.joinTable.get(joinIndex), startFlag, createTable.allTable, true, updateSchema);
				startFlag = false;
				if (temp2.length() == 0)
					break;
				joinIndex++;

				for (int i = 2; i < scanners.size(); i++) {
					temp2 = parseSelect.joinRowAndTable(temp2, scanners.get(i), parseSelect.fromTable.get(i), parseSelect.joinTable.get(joinIndex), parseSelect.schema, false, updateSchema);
					if (temp2.length() == 0)
						break;
					joinIndex++;
				}
			} else {
				temp2 = scanners.get(0).scanFile();
			}
			if (temp2.length() == 0)
				break;
			// after joining once, no need to update schema
			stopFlag = scanners.get(0).isFlag();
			updateSchema = false;

			/* where condition */
			if (parseSelect.whereCondition.size() != 0) {
				for (String s : parseSelect.whereCondition) {
					temp2 = select.selectRow(parseSelect.schema, temp2, s);
					// not meet the select requirement
					if (temp2 == null) {
						break;
					}
				}
			}	
			
			/* group by */
			if (parseSelect.groupByTable != null && parseSelect.groupByTable.get(0) != null) {
				groupByFlag = true;
				result = group.groupBy(result, parseSelect.schema, temp2, parseSelect.groupByTable.get(0));
			}
			
			/* order by */
			if (parseSelect.groupByTable != null && parseSelect.orderByTable.get(0) != null) {
				if (groupByFlag) {
					// orderBy2(ArrayList<StringBuilder> result, HashMap<String, HashMap<String, String>> scheme,String query,StringBuilder singleRecord )
					result = Orderby2.orderBy2(result, parseSelect.schema, parseSelect.orderByTable.get(0), temp2);
				} else {
					result = Orderby2.orderBy2(result, parseSelect.schema, parseSelect.orderByTable.get(0), temp2);
				}		
			}
			/* projection */
			if (!groupByFlag) {
				query = parseSelect.projectTable.toString().substring(1, parseSelect.projectTable.toString().length() - 1);
				result = projection.proTable(result, parseSelect.schema, temp2, query);
			}
			/* distinct */
			if (parseSelect.distinctTable != null) {
				query = parseSelect.distinctTable.toString().substring(1, parseSelect.distinctTable.toString().length() - 1);
				result = distinct.dist(parseSelect.schema, result, query.toString());
			}
		}
		
		/* having */
		if (!parseSelect.havingCondition.isEmpty()) {
			// havingSelect(HashMap<String, HashMap<String, String>> schema, ArrayList<StringBuilder> result, String aggQuery, String groupQuery)
			having.havingSelect(parseSelect.schema, result, parseSelect.havingCondition.get(0).toString().toLowerCase(), parseSelect.groupByTable.get(0));
		}
		
		/* projection if there has group by */
		if (groupByFlag) {
			query = parseSelect.projectTable.toString().substring(1, parseSelect.projectTable.toString().length() - 1);
			result = projection.proTable(result, parseSelect.schema, query);
		}
//		
//		/* distinct */
//		if (groupByFlag && distinctTable != null) {
//			query = distinctTable.toString().substring(1, distinctTable.toString().length() - 1);
//			result = distinct.dist(parseSelect.schema, result, query.toString());
//		}
		return result;
	}
}
