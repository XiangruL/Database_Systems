package sql.evaluator;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.statement.select.SelectBody;

public class processPlainSelect {
	/** generate relation algebra **/
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
		boolean groupByFlag = false;
		ArrayList<scan> scanners = new ArrayList<scan>();
		ArrayList<StringBuilder> result = new ArrayList<StringBuilder>();
		String query = "";
		for (String s : fromTable) {
			scan current = new scan(s);
			scanners.add(current);
		}
		stopFlag = scanners.get(0).isFlag();
		
		while (!stopFlag) {
			parseSelect.schema = createTable.allTable;
			StringBuilder temp2 = new StringBuilder();
			int joinIndex = 0;
			
			/* join */
			if (joinTable != null && !joinTable.isEmpty()) {
				temp2 = parseSelect.joinTwoTable(scanners.get(0), scanners.get(1), joinTable.get(joinIndex), startFlag, createTable.allTable, true, updateSchema);
				startFlag = false;
				if (temp2.length() == 0)
					break;
				joinIndex++;
				for (int i = 2; i < scanners.size(); i++) {
					temp2 = parseSelect.joinRowAndTable(temp2, scanners.get(i), fromTable.get(i), joinTable.get(joinIndex), parseSelect.schema, false, updateSchema);
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
			if (whereCondition.size() != 0) {
				for (String s : whereCondition) {
					String[] whereS = s.split(" ");
					if (whereS.length == 2 && whereS[1].equals("in")) {
						// InSelect(HashMap<String, HashMap<String, String>> schema, StringBuilder row, String query, ArrayList<StringBuilder> result)
						temp2 = select.InSelect(parseSelect.schema, temp2, s, parseSelect.subRes);
					} else {
						temp2 = select.selectRow(parseSelect.schema, temp2, s);
					}
					// not meet the select requirement
					if (temp2 == null) {
						break;
					}
				}
			}	
			
			
			
			/* group by */
			if (groupByTable != null && groupByTable.get(0) != null) {
				groupByFlag = true;
				result = group.groupBy(result, parseSelect.schema, temp2, groupByTable.get(0));
			}
			
			/* order by */
			if (groupByTable != null && orderByTable.get(0) != null) {
				if (groupByFlag) {
					// orderBy2(ArrayList<StringBuilder> result, HashMap<String, HashMap<String, String>> scheme,String query,StringBuilder singleRecord )
					// result = Orderby2.orderBy2(result, parseSelect.schema, orderByTable.get(0), temp2);
					result = order.orderBy(parseSelect.schema, result, temp2, orderByTable.get(0), groupByTable.get(0));

				} else {
					// orderBy(HashMap<String, HashMap<String, String>> schema, ArrayList<StringBuilder> result, String orderQuery,StringBuilder row )
					//result = Orderby2.orderBy2g(result, parseSelect.schema, orderByTable.get(0), temp2, query);
				}		
			}
			/* projection */
			if (!groupByFlag) {
				query = projectTable.toString().substring(1, projectTable.toString().length() - 1);
				if (distinctTable != null) {
					result = projection.proTable(result, parseSelect.schema, temp2, query, true);
				} else {
					result = projection.proTable(result, parseSelect.schema, temp2, query, false);
				}
			}
		}
		
		/* having */
		if (!havingCondition.isEmpty()) {
			// havingSelect(HashMap<String, HashMap<String, String>> schema, ArrayList<StringBuilder> result, String aggQuery, String groupQuery)
			having.havingSelect(parseSelect.schema, result, havingCondition.get(0).toString().toLowerCase(), groupByTable.get(0));
		}
		
		/* projection if there has group by */
		if (groupByFlag) {
			query = projectTable.toString().substring(1, projectTable.toString().length() - 1);
			if (distinctTable != null) {
				result = projection.proTable(result, parseSelect.schema, query, true);
			} else {
				result = projection.proTable(result, parseSelect.schema, query, false);
			}
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
