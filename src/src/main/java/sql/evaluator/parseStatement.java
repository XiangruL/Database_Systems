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
		/* refresh all parameters */
		resetParameters.resetAll();
		
		System.out.println("===========================================================================================");
		System.out.println("Query evaluation complete. Following is the result: ");
		
		/* when statement is union */
		if (selectBd instanceof SetOperationList) {
			List<SelectBody> selects = ((SetOperationList) selectBd).getSelects();
			parseSelect p1 = new parseSelect();
			p1.splitStatement(selects.get(0));
			ArrayList<StringBuilder> tempRes1 = p1.subRes;
			parseSelect p2 = new parseSelect();
			p2.splitStatement(selects.get(1));
			ArrayList<StringBuilder> tempRes2 = p2.subRes;
			ArrayList<StringBuilder> tempRes = union.unionTable(tempRes1, tempRes2);


			printOutResult.printOutFinalResult(tempRes);
		/* when statement is plain select */
		} else {
			parseSelect p1 = new parseSelect();
			ArrayList<StringBuilder> tempRes = p1.splitStatement(selectBd);
			printOutResult.printQueryResult(p1.res);
			System.out.println("");
			printOutResult.printOutFinalResult(p1.subRes);
		}

		
		
	}

}
