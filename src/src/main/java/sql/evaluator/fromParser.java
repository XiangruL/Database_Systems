package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;

public class fromParser {

	public static void parseFrom(parseSelect p, FromItem fromItem, List<Join> joinList) {

		/* get the first from item */
		StringBuilder from = new StringBuilder();
		from.append("FROM: ");
		// has alias in from statement
		if (fromItem.getAlias() != null && fromItem.getAlias().toString().length() != 0) {
			// has alias, put it in alias-name map
			p.setAliasFlag(true);
			String temp = fromItem.toString().toLowerCase();
			p.aliasNameMap.put((temp.split("\\s+"))[1], (temp.split("\\s+"))[0]);
			// no alias, change it to lower case and put it into from 
		} else {
			p.fromTable.add(fromItem.toString().toLowerCase());
		}
		
		/** get joins **/
		// if no join condition
		if (joinList == null) {
			p.joins.add(null);
		} else {
			for (Join o : joinList) {
				// join has alias
				if ((o.toString().split("\\s+")).length > 1) {
					String temp = o.toString().toLowerCase();
					p.aliasNameMap.put((temp.split("\\s+"))[1], (temp.split("\\s+"))[0]);
				}
			}
		}
		
		/** after having all names and alias print all from item **/
		for (String st : p.aliasNameMap.keySet()) {
			p.fromTable.add(p.aliasNameMap.get(st));
		}	
		from.append(p.fromTable.toString().substring(1, p.fromTable.toString().length() - 1));
		from.append("\n");
		p.res.add(from);
	}
	
	
}
