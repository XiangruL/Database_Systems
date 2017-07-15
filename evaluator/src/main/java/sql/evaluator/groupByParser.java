package sql.evaluator;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;

public class groupByParser {
	
	public static void parseGroupBy(parseSelect p, List<Expression> groupByItemList) {
		StringBuilder groupBy = new StringBuilder();
		groupBy.append("GROUP-BY: ");

		if (groupByItemList == null) {
			p.groupByTable.add(null);
		} else {
			for (Expression o : groupByItemList) {
				StringBuilder temp = new StringBuilder();
				temp = p.aliasToName(o, temp);
				p.groupByTable.add(temp.toString());
			}
		}
		groupBy.append(p.groupByTable.toString().substring(1, p.groupByTable.toString().length() - 1));
		groupBy.append("\n");
		p.res.add(4, groupBy);
	}
}
