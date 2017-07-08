package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SetOperationList;

public class parseStatement {
	
	public static void parsingWorkFlow(Statement stmt) {
		SelectBody selectBd = ((Select) stmt).getSelectBody();
		parseSelect.aliasNameMap = new HashMap<String, String>();
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
		
		System.out.println("===========================================================================================");
		System.out.println("Query evaluation complete. Following is the result: ");
		
		if (selectBd instanceof SetOperationList) {
			List<SelectBody> selects = ((SetOperationList) selectBd).getSelects();
			System.out.println(selects);
			return;
		}
		
//		parseSelect.splitStatement(selectBd, false);
		
		
	}

}
