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
			ArrayList<StringBuilder> tempRes1 = parseSelect.splitStatement(selects.get(0), false);
			resetParameters.resetAll();
			ArrayList<StringBuilder> tempRes2 = parseSelect.splitStatement(selects.get(1), false);
			ArrayList<StringBuilder> tempRes = union.unionTable(tempRes1, tempRes2);

			printOutResult.printQueryResult(parseSelect.res);
			System.out.println("");
			printOutResult.printOutFinalResult(tempRes);
		/* when statement is plain select */
		} else {

			ArrayList<StringBuilder> tempRes = parseSelect.splitStatement(selectBd, false);
			printOutResult.printQueryResult(parseSelect.res);
			System.out.println("");
			printOutResult.printOutFinalResult(tempRes);
		}

		
		
	}

}
