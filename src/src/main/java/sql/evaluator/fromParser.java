package sql.evaluator;

import java.util.List;

import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;

public class fromParser {

	public static void parseFrom(FromItem fromItem, List<Join> joinList) {

		/* get the first from item */
//		StringBuilder from = new StringBuilder();
//		from.append("FROM: ");
		// has alias in from statement
		if (fromItem.getAlias() != null && fromItem.getAlias().toString().length() != 0) {
			// has alias, put it in alias-name map
			parseSelect.aliasFlag = true;
			String temp = fromItem.toString().toLowerCase();
			parseSelect.aliasNameMap.put((temp.split("\\s+"))[1], (temp.split("\\s+"))[0]);
			// no alias, change it to lower case and put it into from 
			parseSelect.fromTable.add(fromItem.toString().toLowerCase());
		}
		
		/** get joins **/
		StringBuilder join = new StringBuilder();
		join.append("JOIN: ");
		// if no join condition
		if (joinList == null) {
			join.append(joinList);
		} else {
			for (Join o : joinList) {
				// join has alias
				if ((o.toString().split("\\s+")).length > 1) {
					String temp = o.toString().toLowerCase();
					parseSelect.aliasNameMap.put((temp.split("\\s+"))[1], (temp.split("\\s+"))[0]);
				}
			}
		}
		
		/** after having all names and alias print all from item **/
		for (String st : parseSelect.aliasNameMap.keySet()) {
			parseSelect.fromTable.add(parseSelect.aliasNameMap.get(st));
		}	
//		from.append(parseSelect.fromTable.toString().substring(1, parseSelect.fromTable.toString().length() - 1));
//		from.append("\n");
	}
	
	
}
