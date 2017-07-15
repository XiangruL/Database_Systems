package sql.evaluator;

import java.util.List;

import net.sf.jsqlparser.statement.select.OrderByElement;

public class orderByParser {
	
	public static void parseOrderBy(parseSelect p, List<OrderByElement> orderByItemList) {
		StringBuilder orderBy = new StringBuilder();
		orderBy.append("ORDER-BY: ");
		if (orderByItemList == null) {
			p.orderByTable.add(null);
		} else {
			for (OrderByElement o : orderByItemList) {
				StringBuilder temp = new StringBuilder();
				temp =  p.aliasToName(o, temp);
				p.orderByTable.add(temp.toString());
			}
		}
		orderBy.append(p.orderByTable.toString().substring(1, p.orderByTable.toString().length() - 1));
		orderBy.append("\n");
		p.res.add(5, orderBy);

	}

}
