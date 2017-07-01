package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
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
	private static HashMap<String, HashMap<String, String>>  schema = createTable.allTable;
	private static final Logger LOGGER = Logger.getLogger( parserTest.class.getName() );
	/* containing join condition, "table1.PID = table2.ID" */
	private static ArrayList<String> joinTable;
	private static ArrayList<Expression> condition;
	private static ArrayList<String> whereCondition;
	private static ArrayList<Expression> havingCondition;
	private static ArrayList<String> fromTable;
	private static ArrayList<String> projectTable;

	public static void splitStatement(Statement stmt) {
		/* store select statement output information
		 * format is: PROJECTION: R.A, R.B, R.C
		 * 			  FROM: R, S
		 *            SELECTION: R.A, R.B, R.C
		 *            JOIN: R.A, S.D
		 *            GROUP-BY: R.A
		 *            ORDER-BY: R.C
		 */
		SelectBody selectBd = ((Select) stmt).getSelectBody(); 
		/* mapping alias to full table name
		 * key - alias, value - table name
		 */
		aliasNameMap = new HashMap<String, String>();
		/* alias status */
		boolean aliasFlag = false; 
		/* from item - the first table in from statement */
		FromItem fromItem = ((PlainSelect) selectBd).getFromItem();
		/* joins list */
		List<Join> joinList = ((PlainSelect) selectBd).getJoins(); 
		/* selection list */
		List<SelectItem> selectItemList =  ((PlainSelect) selectBd).getSelectItems();
		/* group by list */
		List<Expression> groupByItemList = ((PlainSelect) selectBd).getGroupByColumnReferences();
		/* order by list */
		List<OrderByElement> orderByItemList = ((PlainSelect) selectBd).getOrderByElements();
		/* where conditions */
		Expression whereEx = ((PlainSelect) selectBd).getWhere();
		/* having conditions */
		Expression havingEx = ((PlainSelect) selectBd).getHaving();
		
		List<Expression> whereList = new ArrayList<Expression>();
		
		condition = new ArrayList<Expression>();
		whereCondition = new ArrayList<String>();
		havingCondition = new ArrayList<Expression>(); 
		joinTable = new ArrayList<String>();
		projectTable = new ArrayList<String>();
		
		/** get from item **/
		StringBuilder from = new StringBuilder();
		from.append("FROM: ");
		fromTable= new ArrayList<String>();
		// has alias in from statement
		if (fromItem.getAlias() != null && fromItem.getAlias().toString().length() != 0) {
			// has alias, put it in alias-name map
			aliasFlag = true;
			String temp = fromItem.toString().toLowerCase();
			aliasNameMap.put((temp.split("\\s+"))[1], (temp.split("\\s+"))[0]);
		} else {
			// no alias, change it to lower case and put it into from 
			fromTable.add(fromItem.toString().toLowerCase());
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
					aliasNameMap.put((temp.split("\\s+"))[1], (temp.split("\\s+"))[0]);
				}
			}
		}
		
		
		/** after having all names and alias print all from item **/
		for (String st : aliasNameMap.keySet()) {
			fromTable.add(aliasNameMap.get(st));
		}
		from.append(fromTable.toString().substring(1, fromTable.toString().length() - 1));
		from.append("\n");

		
		/** get select item **/
		StringBuilder select = new StringBuilder();
		StringBuilder project = new StringBuilder();
		String prefix = "";
		for (SelectItem o : selectItemList) {
			// select *
			if (aliasFlag == true) {
				StringBuilder temp = new StringBuilder();
				select = aliasToName(o,select);
				temp = aliasToName(o, temp);
				projectTable.add(temp.toString());
			} else {
				if (o.toString().contains("*")) {
					if (createTable.allTable.get((o.toString().toLowerCase().split("\\."))[0]) != null) {
						// fullname.*
						for (String s : createTable.allTable.get((o.toString().toLowerCase().split("\\."))[0]).keySet()) {
							select.append(prefix);
							prefix = ", ";
							select.append(s);
							projectTable.add(s);		
						}
						// *
					} else {
						for (String s : createTable.allTable.get(fromItem.toString().toLowerCase()).keySet()) {
							select.append(prefix);
							prefix = ", ";
							select.append(s);
							projectTable.add(s);
						}
					}
				} else {
					select.append(prefix);
					prefix = ", ";
					select.append(o.toString().toLowerCase());
					projectTable.add(o.toString().toLowerCase());
				}
			}
		}
		select.insert(0, "SELECTION: ");
		project.append("PROJECTION: ");
		project.append(projectTable.toString().substring(1, projectTable.toString().length() - 1));
		
		select.append("\n");
		project.append("\n");
		
		
		/** get group-by item **/
		StringBuilder groupBy = new StringBuilder();
		groupBy.append("GROUP-BY: ");
		if (groupByItemList == null) {
			groupBy.append(groupByItemList);
		} else {
			prefix = "";
			for (Expression o : groupByItemList) {
				groupBy.append(prefix);
				prefix = ", ";
				if (aliasFlag == true) {
					groupBy = aliasToName(o, groupBy);
				} else {
					groupBy.append(o.toString().toLowerCase());
				}

			}
		}
		groupBy.append("\n");
		
		
		/** get order-by item  **/
		StringBuilder orderBy = new StringBuilder();
		orderBy.append("ORDER-BY: ");
		if (orderByItemList == null) {
			orderBy.append(orderByItemList);
		} else {
			prefix = "";
			for (OrderByElement o : orderByItemList) {
				orderBy.append(prefix);
				prefix = ", ";
				if (aliasFlag == true) {
					orderBy = aliasToName(o, orderBy);
				} else {
					orderBy.append(o.toString().toLowerCase());
				}
			}
		}
		orderBy.append("\n");
		
		
		/** get where **/
		StringBuilder where = new StringBuilder();
		// where contains and
		if (whereEx instanceof AndExpression) {
			whereList = visit((AndExpression) whereEx);
			for (Expression e : whereList) {		
				if (e instanceof EqualsTo) {
					join = parseEqualTo(e, aliasFlag, join);
				} else {
					condition.add(e);
				}
				where.append(e.toString().toLowerCase());
				where.append("\n");
			}
		// there is a single where statement
		} else {
			if (whereEx instanceof EqualsTo) {
				join = parseEqualTo(whereEx, aliasFlag, join);
			} else {
				if (whereEx != null) {
					where.append(whereEx.toString().toLowerCase());
					condition.add(whereEx);
				} else {
					// no where statement
					where.append(whereEx);
				}
			}
		}
		for (Expression e : condition) {
			whereCondition.add(parseWhereCondition(e));
		}
		
		
		
		/** get having **/
		StringBuilder having = new StringBuilder();
		// having contains and
		if (havingEx instanceof AndExpression) {
			havingCondition = (ArrayList<Expression>) visit((AndExpression) havingEx);
		// there is a single where statement
		} else if (havingEx instanceof OrExpression) {
			havingCondition = (ArrayList<Expression>) visitOr((OrExpression) havingEx);
		} else {
			havingCondition.add(havingEx);
		}

		/** Print out the query evaluated result **/
		StringBuilder res = new StringBuilder();
		res.append(project);
		res.append(from);
		res.append(select);
		if (join.charAt(join.length() - 2) == ',')
			join.replace(join.length() - 2, join.length(), "\n");
		
		else  {
			join.append("\n");
		}
		res.append(join);
		res.append(groupBy);
		res.append(orderBy);
//		res.append(whereCondition);
//		res.append(where);
//		res.append(whereCondition.toString());
//		res.append(havingCondition.toString());
		System.out.println(res.toString());
		
		parseSelect.algebraGen();
		
	}
	
	public static StringBuilder aliasToName(Object o, StringBuilder s) {
		// has alias and can split
		if (o == null) 
			return null;
		String temp = o.toString().toLowerCase();
		if (aliasNameMap.get((temp.split("\\."))[0]) != null) {
			// contains *
			if ((temp.split("\\."))[1].equals("*")) {		
				// print out all columns extracted from the table info
				String prefix = "";
				for (String s1 : createTable.allTable.get(aliasNameMap.get((temp.split("\\."))[0])).keySet()) {
					s.append(prefix);
					s.append(aliasNameMap.get((temp.split("\\."))[0]));
					s.append(".");
					s.append(s1);
					prefix = ", ";
				}
			// only contain a column name
			} else {
				s.append(aliasNameMap.get((temp.split("\\."))[0]));
				s.append(".");
				s.append((temp.split("\\."))[1]);
			}
		// handle non alias input
		} else {
			if (createTable.allTable.get((temp.split("\\."))[0]) != null) {
				s.append(temp);
			} else {
				System.out.println("cannot find tthis table name");
			}
		}
//		s.append(" ");
		return s;
	}
	
	
	/** generate relation algebra **/
	public static void algebraGen() {
		// no join
		boolean startFlag = true;
		boolean stopFlag = true;
		boolean stopFlag2 = true;
		ArrayList<scan> scanners = new ArrayList<scan>();
		ArrayList<StringBuilder> result = new ArrayList<StringBuilder>();
		String query = "";
//		for (String s : fromTable) {
//			scan current = new scan(s);
//			scanners.add(current);
//			stopFlag &= current.isFlag();
//		}
		scan scanner1 = new scan(fromTable.get(0));
		scan scanner2 = new scan(fromTable.get(1));
		// when table1 and table2 at the bottom, stopFlag becomes true
		stopFlag = scanner1.isFlag() & scanner2.isFlag();

		while (!stopFlag) {
			StringBuilder temp2 = joinTwoTable(scanner1, scanner2, joinTable.get(0), startFlag);
			stopFlag = scanner1.isFlag() & scanner2.isFlag();
			startFlag = false;

			/* where condition */
			if (whereCondition.size() != 0) {
				for (String s : whereCondition) {
					temp2 = select.selectRow(schema, temp2, s);
					//						System.out.println(temp);

					// not meet the select requirement
					if (temp2 == null) {
						break;
					}
				}
			}

			/* projection */
			query = projectTable.toString().substring(1, projectTable.toString().length() - 1);
			result = projection.proTable(result, schema, temp2, query, -1);
		}

		for (StringBuilder s : result)
			System.out.println(s.toString());
	}

	
	/** parse where/having statement containing and
	 *  return a list of seperated condition **/
	public static List<Expression> visit(AndExpression expr) {
		List<Expression> list = new ArrayList<Expression>();
		list.add(expr.getRightExpression());
		Expression ex = expr.getLeftExpression();
        while(ex instanceof AndExpression) { 
        	list.add(((BinaryExpression) ex).getRightExpression());
        	ex = ((BinaryExpression) ex).getLeftExpression();
        }
        list.add((BinaryExpression) ex);
        return list;
    }
	
	/** parse where/having statement containing or
	 *  return a list of seperated condition **/
	public static List<Expression> visitOr(OrExpression expr) {
		List<Expression> list = new ArrayList<Expression>();
		list.add(expr.getRightExpression());
		Expression ex = expr.getLeftExpression();
        while(ex instanceof OrExpression) { 
        	list.add(((BinaryExpression) ex).getRightExpression());
        	ex = ((BinaryExpression) ex).getLeftExpression();
        }
        list.add((BinaryExpression) ex);
        return list;
    }
	
	/** Parse where expression containing "="
	 * tell join condition and select condition apart
	 * @param e - where expression
	 * @param aliasFlag - true means alias is used
	 * @param join - if there is a join condition, it needs to put it into join stringbuilder
	 * @return new updated join stringbuilder
	 */
	public static StringBuilder parseEqualTo(Expression e, boolean aliasFlag, StringBuilder join) {
		// get right expression 
		StringBuilder tempWhere = new StringBuilder();
		StringBuilder tempWhereL = new StringBuilder();
		StringBuilder tempWhereR = new StringBuilder();
		Expression rightE = ((EqualsTo) e).getRightExpression();
		Expression leftE = ((EqualsTo) e).getLeftExpression();
		if (aliasFlag == true) {
			// can find alias-name pair
			if (aliasNameMap.get((rightE.toString().toLowerCase().split("\\."))[0]) != null) {
				tempWhereL = aliasToName(leftE.toString(), tempWhereL);		
				tempWhereR = aliasToName(rightE.toString(), tempWhereR);
				tempWhere.append(tempWhereL);
				tempWhere.append(" = ");
				tempWhere.append(tempWhereR);
				joinTable.add(tempWhere.toString().toLowerCase());
				// left expression doesn't show in join yet
				if (!join.toString().contains(tempWhereL.toString())) {
					join.append(tempWhereL);
					join.append(", ");
				}
				// right expression doesn't show in join yet
				if (!join.toString().contains(tempWhereR.toString())) {
					join.append(tempWhereR);
					join.append(", ");
				}
			} else {
				// it's selection condition not for joining
				condition.add(e);
			}
		} else {
			// right expression contains ".", means it's join condition
			String temp = rightE.toString().toLowerCase();
			String temp2 = leftE.toString().toLowerCase();
			if (createTable.allTable.get((temp.split("\\."))[0]) != null) {
				joinTable.add(e.toString().toLowerCase());
				// left expression doesn't show in join yet
				if (!join.toString().contains(temp)) {
					join.append(temp);
					join.append(", ");
				}
				// right expression doesn't show in join yet
				if (!join.toString().contains(temp2)) {
					join.append(temp2);
					join.append(", ");
				}
			} else {
				condition.add(e);
			}
		}
		return join;
	}

	/** parse where condition
	 * if there is alias in the left expression, change it to full name 
	 * 
	 * @param s - where condition, eg "T.name = 1"
	 * @return - return full name with condition, eg "table.name = 1"
	 */
	public static String parseWhereCondition(Expression s) {
		if (s instanceof EqualsTo) {
			Expression rightE = ((EqualsTo) s).getRightExpression();
			Expression leftE = ((EqualsTo) s).getLeftExpression();
			StringBuilder temp = new StringBuilder();
			temp = aliasToName(leftE, temp);
			temp.append(" = ");
			temp.append(rightE.toString());
			return temp.toString();		
		} else if (s instanceof GreaterThan) {
			
		} else if (s instanceof GreaterThanEquals) {
			
		} else if (s instanceof LikeExpression) {
			
		} else {
			
		}
		return s.toString();
	}
	
	/** 
	 * @return fromTable containing from item
	 */
	public static ArrayList<String> getFromTable() {
		return fromTable;
	}
	
	
	/** This method use to join two tables, return the first row match the joining condition
	 * 
	 * @param scanner1 - scan for table 1
	 * @param scanner2 - scan for table 2
	 * @param query - join condition, ex "t1.a = t2.b"
	 * @param startFlag - true if it's first time to enter the join
	 * @return
	 */
	public static StringBuilder joinTwoTable(scan scanner1, scan scanner2, String query, boolean startFlag) {
		boolean flag = scanner1.isFlag();
		StringBuilder temp = new StringBuilder();
		while (!scanner1.isFlag()) {
			// table1 first enter the join method, need to scan a line
			if (startFlag) {
				temp = scanner1.scanFile();
			} else {
				// table1 need to scan a line when scanner 2 reach the bottom
				if (scanner2.isFlag()) {
					temp = scanner1.scanFile();
				}
			}
			// table1 not at the bottom but table2 do, loop table2
			if (!scanner1.isFlag() && scanner2.isFlag()) {		
				scanner2.setFlag(false);
			}
			// table2 does not reach the bottom
			while(!scanner2.isFlag()) {
				StringBuilder temp2 = new StringBuilder();
				temp2 = join.joinRow(createTable.allTable, temp, scanner2.scanFile(), query, flag);
				if (!flag) {
					parseSelect.schema = join.getNewSchema();
					flag = true;
				}
				if (temp2 != null && temp2.length() > 0) {
					return temp2;
				}
			}
		}
		// no match at these two table, return an empty string builder
		return new StringBuilder();
	}
}
