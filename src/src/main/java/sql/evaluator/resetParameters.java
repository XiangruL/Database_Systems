package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectBody;

public class resetParameters {
	
	/* Reset all parameters when start a new query */
	public static void resetAll() {
		parseSelect.aliasNameMap = new HashMap<String, String>();
		parseSelect.subAliasNameMap = new HashMap<String, String>();
		parseSelect.schema = createTable.allTable;
		parseSelect.joinTable = new ArrayList<String>();
		parseSelect.condition = new ArrayList<Expression>();
		parseSelect.whereCondition = new ArrayList<String>();
		parseSelect.havingCondition = new ArrayList<String>();
		parseSelect.groupByTable = new ArrayList<String>();
		parseSelect.orderByTable = new ArrayList<String>();
		parseSelect.fromTable = new ArrayList<String>();
		parseSelect.projectTable = new ArrayList<String>();
		parseSelect.distinctTable = new ArrayList<String>();
		parseSelect.subQueryTable = new ArrayList<SelectBody>();
		parseSelect.res = new ArrayList<StringBuilder>();
		parseSelect.subRes = new ArrayList<StringBuilder>();
	}
}
