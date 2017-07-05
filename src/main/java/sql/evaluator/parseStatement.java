package sql.evaluator;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

public class parseStatement {
	
	public static void parsingWorkFlow(Statement stmt) {
		SelectBody selectBd = ((Select) stmt).getSelectBody();
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
		
		parseSelect.splitStatement(selectBd, false);
		
		
	}

}
