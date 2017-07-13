package sql.evaluator;

import net.sf.jsqlparser.expression.Expression;

public class havingParser {
	
	public static void parseHaving(parseSelect p, Expression havingEx) {
		/** get having **/
		// having contains and
		if (havingEx != null){
			p.havingCondition.add(whereParser.parseWhereCondition(p, havingEx));
		}
	}

}
