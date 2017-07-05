package sql.evaluator;

import java.util.ArrayList;

public class printOutResult {
	
	/* print out the rows required from the query */
	public static void printOutFinalResult(ArrayList<StringBuilder> result) {
		for (StringBuilder s : result)
			System.out.println(s.toString());
	}
	
	public static void printQueryResult(ArrayList<StringBuilder> result) {
		for (StringBuilder s : result)
			System.out.print(s.toString());
	}
}
