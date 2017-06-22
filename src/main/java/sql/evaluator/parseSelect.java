package sql.evaluator;

import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
public class parseSelect {
	
	private static HashMap<String, String> aliasNameMap = new HashMap<String, String>();
	private static final Logger LOGGER = Logger.getLogger( parserTest.class.getName() );

	
	public static String splitStatement(Statement stmt) {
		/* store select statement output information
		 * format is: PROJECTION: R.A, R.B, R.C
		 * 			  FROM: R, S
		 *            SELECTION: R.A, R.B, R.C
		 *            JOIN: R.A, S.D
		 *            GROUP-BY: R.A
		 *            ORDER-BY: R.C
		 */
		SelectBody selectBd = ((Select) stmt).getSelectBody(); 
		aliasNameMap = new HashMap<String, String>();
		boolean aliasFlag = false; // didn't have a alias
		
		/** get from item **/
		StringBuilder from = new StringBuilder();
		FromItem fromItem = ((PlainSelect) selectBd).getFromItem();
		from.append("FROM: ");
		// has alias in from statement
		if (fromItem.getAlias() != null && fromItem.getAlias().toString().length() != 0) {
			// has alias, put it in alias-name map
			aliasFlag = true;
			aliasNameMap.put((fromItem.toString().split("\\s+"))[1], (fromItem.toString().split("\\s+"))[0]);
		} else {
			from.append(fromItem.toString());
		}
		
		
		/** get joins **/
		StringBuilder join = new StringBuilder();
		List<Join> joinList = ((PlainSelect) selectBd).getJoins(); 
		join.append("JOIN: ");
		// if no join condition
		if (joinList == null) {
			join.append("NULL");
		} else {
			for (Join o : joinList) {
				join.append((o.toString().split("\\s+"))[0]);
				join.append(" ");
				// join has alias
				if ((o.toString().split("\\s+")).length > 1) {
					aliasNameMap.put((o.toString().split("\\s+"))[1], (o.toString().split("\\s+"))[0]);
				}
			}
		}
		join.append("\n");
		

		
		/** get select item **/
		List<SelectItem> selectItemList =  ((PlainSelect) selectBd).getSelectItems();
		StringBuilder select = new StringBuilder();
		StringBuilder project = new StringBuilder();
		select.append("SELCTION: ");
		project.append("PROJECTION: ");
		for (SelectItem o : selectItemList) {
			// select *
			if (aliasFlag == true) {
				select = aliasToName(o,select);
				project = aliasToName(o, project);
			} else {
				for (String s : createTable.allTable.get(fromItem.toString()).keySet()) {
					select.append(s);
					select.append(" ");
					project.append(s);
					project.append(" ");
				}
			}
		}
		select.append("\n");
		project.append("\n");
		
		// after having all names and alias print all from item
		for (String st : aliasNameMap.keySet()) {
			from.append(aliasNameMap.get(st));
			from.append(" ");
		}
		from.append("\n");
		
		
		/** get group-by item **/
		List<Expression> groupByItemList = ((PlainSelect) selectBd).getGroupByColumnReferences();
		StringBuilder groupBy = new StringBuilder();
		groupBy.append("GROUP-BY: ");
		if (groupByItemList == null) {
			groupBy.append("NULL");
		} else {
			for (Expression o : groupByItemList) {
				if (aliasFlag == true) {
					groupBy.append(aliasNameMap.get((o.toString().split("\\."))[0]));
					groupBy.append(".");
					groupBy.append((o.toString().split("\\."))[1]);
				} else {
					groupBy.append(o.toString());
				}
				groupBy.append(" ");
			}
		}
		groupBy.append("\n");
		
		
		/** get order-by item  **/
		List<OrderByElement> orderByItemList = ((PlainSelect) selectBd).getOrderByElements();
		StringBuilder orderBy = new StringBuilder();
		orderBy.append("ORDER-BY: ");
		if (orderByItemList == null) {
			orderBy.append("NULL");
		} else {
			for (OrderByElement o : orderByItemList) {
				if (aliasFlag == true) {
					orderBy = aliasToName(o, orderBy);
				} else {
					orderBy.append(o.toString());
				}
			}
		}
		orderBy.append("\n");

		StringBuilder res = new StringBuilder();
		res.append(project);
		res.append(from);
		res.append(select);
		res.append(join);
		res.append(groupBy);
		res.append(orderBy);
		
		return res.toString();
	}
	
	public static StringBuilder aliasToName(Object o, StringBuilder s) {
		// has alias and can split
		if (aliasNameMap.get((o.toString().split("\\."))[0]) != null) {
			// contains *
			if ((o.toString().split("\\."))[1].equals("*")) {		
				// print out all columns extracted from the table info
				for (String s1 : createTable.allTable.get(aliasNameMap.get((o.toString().split("\\."))[0])).keySet()) {
					s.append(aliasNameMap.get((o.toString().split("\\."))[0]));
					s.append(".");
					s.append(s1);
					s.append(" ");
				}
			// only contain a column name
			} else {
				s.append(aliasNameMap.get((o.toString().split("\\."))[0]));
				s.append(".");
				s.append((o.toString().split("\\."))[1]);
			}
		// cannot handle non alias input
		} else {
			LOGGER.log(Level.SEVERE, "Wrong sql syntax");
		}
		s.append(" ");
		return s;
	}
}
