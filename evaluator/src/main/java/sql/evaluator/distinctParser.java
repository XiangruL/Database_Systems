package sql.evaluator;

import net.sf.jsqlparser.statement.select.Distinct;

public class distinctParser {
	
	public static void parseDistinct(parseSelect p, Distinct distinctI) {
		if (distinctI != null) {
			if (distinctI.toString().toLowerCase().equals("distinct")) {
				p.distinctTable.add(p.projectTable.get(0));		
			}
		} else {
			p.distinctTable = null;
		}
	}

}
