package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
public class parseSelect {
	
	protected HashMap<String, String> aliasNameMap;
	protected boolean aliasFlag;
	protected HashMap<String, HashMap<String, String>>  schema;
	/* containing join condition, "table1.PID = table2.ID" */
	protected ArrayList<String> joinTable;
	/* containing join tables, "table1, table2 ..." */
	protected ArrayList<String> joins;
	protected ArrayList<Expression> condition;
	protected ArrayList<String> whereCondition;
	protected ArrayList<String> havingCondition;
	protected ArrayList<String> groupByTable;
	protected ArrayList<String> orderByTable;
	protected ArrayList<String> fromTable;
	protected ArrayList<String> projectTable;
	protected ArrayList<String> distinctTable;
	protected ArrayList<SelectBody> subQueryTable;
	protected ArrayList<StringBuilder> res;
	protected ArrayList<StringBuilder> subRes;
	protected boolean subQueryFlag;
	
	public parseSelect() {
		aliasNameMap = new HashMap<String, String>();
		boolean aliasFlag = false;
		schema = new HashMap<String, HashMap<String, String>>(createTable.allTable);
		joinTable = new ArrayList<String>();
		joins = new ArrayList<String>();
		condition = new ArrayList<Expression>();
		whereCondition = new ArrayList<String>();
		havingCondition = new ArrayList<String>();
		groupByTable = new ArrayList<String>();
		orderByTable = new ArrayList<String>();
		fromTable = new ArrayList<String>();
		projectTable = new ArrayList<String>();
		distinctTable = new ArrayList<String>();
		subQueryTable = new ArrayList<SelectBody>();
		res = new ArrayList<StringBuilder>();
		subRes = new ArrayList<StringBuilder>();
		subQueryFlag = false;
	}
	
	
	public ArrayList<StringBuilder> splitStatement(SelectBody selectBd) {
		/* store select statement output information
		 * format is: PROJECTION: R.A, R.B, R.C
		 * 			  FROM: R, S
		 *            SELECTION: R.A, R.B, R.C
		 *            JOIN: R.A, S.D
		 *            GROUP-BY: R.A
		 *            ORDER-BY: R.C
		 */
		/* mapping alias to full table name
		 * key - alias, value - table name
		 */

		
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
		/* distinct item */
		Distinct distinctI = ((PlainSelect) selectBd).getDistinct();
		
		List<Expression> whereList = new ArrayList<Expression>();
	
		/** get from item **/
		fromParser.parseFrom(this, fromItem, joinList);
		
	
		/* encounter nested query */
		if (fromItem != null) {
			for (int i = 0; i < 1; i++) {
				if ( fromItem.toString().contains("(")) {
					System.out.println("PROJECTION: " + selectItemList.toString().substring(1, selectItemList.toString().length() - 1));
					StringBuilder temp = new StringBuilder(joinList.get(i).toString());
					temp.append(", ");
					String prefix = "";
					for (int j = 0; j < joinList.size(); j++) {
						if (j != i) {
							temp.append(prefix);
							prefix = ", ";
							temp.append(joinList.get(j).toString().split(" ")[0]);
						}
					}
					System.out.println("FROM: " + temp);
					System.out.println("SELECTION: " +  selectItemList.toString().substring(1, selectItemList.toString().length() - 1));
					System.out.println("GROUP BY: " + groupByItemList);
					System.out.println("ORDER BY: " + orderByItemList);
					return null;
				}
			}
		}	
		
		if (joinList != null) {
			for (int i = 0; i < joinList.size(); i++) {
				if ( joinList.get(i).toString().contains("(")) {
					System.out.println("PROJECTION: " + selectItemList.toString().substring(1, selectItemList.toString().length() - 1));
					StringBuilder temp = new StringBuilder(joinList.get(i).toString());
					temp.append(", ");
					String prefix = "";
					for (int j = 0; j < joinList.size(); j++) {
						if (j != i) {
							temp.append(prefix);
							prefix = ", ";
							temp.append(joinList.get(j).toString().split(" ")[0]);
						}
					}
					System.out.println("FROM: " + temp);
					System.out.println("SELECTION: " +  selectItemList.toString().substring(1, selectItemList.toString().length() - 1));
					System.out.println("GROUP BY: " + groupByItemList);
					System.out.println("ORDER BY: " + orderByItemList);
					return null;
				}
			}
		}	

		/** get select item **/
		selectParser.parseSelect(this, selectItemList);
		
		/** get where **/
		whereParser.parseWhere(this, whereEx, whereList);
		
		/** get group-by item **/
		groupByParser.parseGroupBy(this, groupByItemList);
			
		/** get order-by item  **/
		orderByParser.parseOrderBy(this, orderByItemList);
				
		/** get having **/
		havingParser.parseHaving(this, havingEx);
		
		/** get distinct **/
		distinctParser.parseDistinct(this, distinctI);
		
		/** sort join table 
		 * This step must be placed before parseSelect.algebraGen() **/
		if (!joinTable.isEmpty()) {
			joinTable = parseSelect.sortJoinTable(joinTable);
			fromTable = parseSelect.sortFromTable(joinTable, fromTable);
		}
	
			
		if (!subQueryTable.isEmpty()) {
			parseSelect subP = new parseSelect();
			subP.setSubQueryFlag(true);
			subP.splitStatement(subQueryTable.get(0));
			this.res.set(5, subP.res.get(5));
			this.res.set(4, subP.res.get(4));
			subRes = processPlainSelect.algebraGen(this);

		}
		else {
			if (this.subQueryFlag) {
//				System.out.println(processPlainSelect.algebraGen(this));
				processPlainSelect.subQueryRes = processPlainSelect.algebraGen(this);

			} else {
				subRes = processPlainSelect.algebraGen(this);
			}
		}
		return null;
	}
	
	public void setAliasFlag(boolean f) {
		this.aliasFlag = f;
	}
	
	public void setSubQueryFlag(boolean f) {
		this.subQueryFlag = f;
	}
	
	/* Enter a subquery */
//	public static void parseSubQuery(SelectBody selectBd) {
//		splitStatement(selectBd, true);
//	}
	
	/** Translate a give alias or name to a full table name
	 * 
	 * @param o - given object
	 * @param s - to append full name in this string builder
	 * @return
	 */
	public StringBuilder aliasToName(Object o, StringBuilder s) {
		// has alias and can split
		if (o == null) 
			return null;
		String temp = o.toString().toLowerCase();
		if (aliasNameMap.get((temp.split("\\."))[0]) != null) {
			// contains *
			if ((temp.split("\\."))[1].equals("*")) {		
				// print out all columns extracted from the table info
//				String prefix = "";
				String[] list = new String[createTable.allTable.get(aliasNameMap.get((temp.split("\\."))[0])).keySet().size()];
				for (String s1 : createTable.allTable.get(aliasNameMap.get((temp.split("\\."))[0])).keySet()) {
//					sb.append(prefix);
					StringBuilder sb = new StringBuilder();
					sb.append(aliasNameMap.get((temp.split("\\."))[0]));
					sb.append(".");
					sb.append(s1);
					int colNum = tool.getColNum(createTable.allTable, aliasNameMap.get((temp.split("\\."))[0]), s1);
					list[colNum] = sb.toString();
//					prefix = ", ";
				}
				String prefix = "";
				for (String t : list) {
					s.append(prefix);
					s.append(t);
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
				s.append(temp);
			}
		}
		return s;
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
        if (ex instanceof InExpression) {
        	list.add((InExpression) ex);
        } else {
        	list.add((BinaryExpression) ex);
        }
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
	
	
	/** This method use to join two tables, return the first row match the joining condition
	 * 
	 * @param scanner1 - scan for table 1
	 * @param scanner2 - scan for table 2
	 * @param query - join condition, ex "t1.a = t2.b"
	 * @param startFlag - true if it's first time to enter the join
	 * @return
	 */
//	public static StringBuilder joinTwoTable(scan scanner1, scan scanner2, String query, boolean startFlag, HashMap<String, HashMap<String, String>> oldschema, boolean joinF, boolean updateSchema) {
//		StringBuilder temp;
//		while (!scanner1.isFlag()) {
//			// table1 first enter the join method, need to scan a line
//			if (startFlag) {
//				temp = scanner1.scanFile();
//			} else {
//				// table1 need to scan a line when scanner 2 reach the bottom
//				if (scanner2.isFlag()) {
//					temp = scanner1.scanFile();
//				}
//			}
//			// table1 not at the bottom but table2 do, loop table2
//			if (!scanner1.isFlag() && scanner2.isFlag()) {		
//				scanner2.setFlag(false);
//			}
//			// table2 does not reach the bottom
//			while(!scanner2.isFlag()) {
//				StringBuilder temp2 = new StringBuilder();
//				//				temp2 = join.joinRow(oldschema, createTable.allTable, null, scanner1.getLine(), scanner2.scanFile(), query, joinF, updateSchema);
//				updateSchema = false;
//				if (temp2 != null && temp2.length() > 0) {
//					parseSelect.schema = join.getNewSchema();
//					return temp2;
//				}
//			}
//		}
//		// no match at these two table, return an empty string builder
//		return new StringBuilder();
//	}
	
	/** join a pre-joined row and a row from a new table
	 * 
	 * @param row - pre-joined row
	 * @param scanner - scanner to scan a row from a new table
	 * @param tableName - a table need to update the schema
	 * @param query - join statement, eg "A.a = B.b"
	 * @param oldschema - schema to look up a column in a pre-joined row
	 * @param joinF - true means two tables join, false means row and a table join
	 * @param updateSchema - true need to update schema
	 * @return a join output row
	 */
//	public static StringBuilder joinRowAndTable(StringBuilder row, scan scanner, String tableName,String query, HashMap<String, HashMap<String, String>> oldschema, boolean joinF, boolean updateSchema) {
//		boolean flag = scanner.isFlag();
//		boolean schemaFlag = false;
//		StringBuilder temp = new StringBuilder();
//		while(!flag) {
//			flag = scanner.isFlag();
//			temp = join.joinRow(oldschema, createTable.allTable, tableName, row, scanner.scanFile(), query, joinF, updateSchema);
//			updateSchema = false;
//			if (temp != null && temp.length() > 0) {
//				parseSelect.schema = join.getNewSchema();
//				return temp;
//			}
//		}
//		return temp;
//	}
	
	
	/** sort join table, so it can use this table directly in joining operation
	 * 
	 * @param joinT - the complete join condition, eg, ["A.a = B.b", "B.a = C.d"]
	 */
	public static ArrayList<String> sortJoinTable(ArrayList<String> joinT) {
		ArrayList<String> newJoinTable = new ArrayList<String>();
		HashSet<String> tableName = new HashSet<String>(); 
		String ts = new String();
		for (String s : joinT) {
			StringBuilder temp = new StringBuilder();
			String[] s1 = s.split(" = ");
			String[] itm1 = s1[0].split("\\.");
			String[] itm2 = s1[1].split("\\.");
			// if at least one table showed before, put this condition in the join table 
			if (tableName.size() == 0 || tableName.contains(itm1[0]) || tableName.contains(itm2[0])) {
				if (tableName.toString().contains(itm1[0])) {
					temp.append(s1[0]);
					temp.append(" = ");
					temp.append(s1[1]);
				} else if (tableName.toString().contains(itm2[0])) {
					temp.append(s1[1]);
					temp.append(" = ");
					temp.append(s1[0]);
				} else {
					temp.append(s);
				}
				tableName.add(itm1[0]);
				tableName.add(itm2[0]);
				newJoinTable.add(temp.toString());
			} else {
				// didn't show before, keep it
				ts = s;
//				joinT.add(s);
			}
		}
		if (ts.length() != 0) {
			StringBuilder temp = new StringBuilder();
			String[] s1 = ts.split(" = ");
			String[] itm1 = s1[0].split("\\.");
			if (tableName.toString().contains(itm1[0])) {
				temp.append(s1[0]);
				temp.append(" = ");
				temp.append(s1[1]);
			} else {
				temp.append(s1[1]);
				temp.append(" = ");
				temp.append(s1[0]);
			}
			newJoinTable.add(temp.toString());
		}
		return newJoinTable;	
	}
	
	/** sort from table, so it can use this table directly in joining operation
	 * 
	 * @param joinT - the complete join condition, eg, ["A.a = B.b", "B.a = C.d"]
	 * so the new from table would be [A, B, C]
	 */
	public static ArrayList<String> sortFromTable(ArrayList<String> joinT, ArrayList<String> fromT) {
		ArrayList<String> newFromTable = new ArrayList<String>();
		HashSet<String> tableName = new HashSet<String>(); 
		for (String s : joinT) {
			String[] s1 = s.split(" = ");
			String[] itm1 = s1[0].split("\\.");
			String[] itm2 = s1[1].split("\\.");
			// if table name is not showed before, put it in the from table 
			if (!tableName.contains(itm1[0])) {
				tableName.add(itm1[0]);
				newFromTable.add(itm1[0]);
			}
			if (!tableName.contains(itm2[0])) {
				tableName.add(itm2[0]);
				newFromTable.add(itm2[0]);
			}
		}
		for (String s : fromT) {
			if (!tableName.contains(s)) {
				tableName.add(s);
				newFromTable.add(s);
			}
		}
		return newFromTable;
	}
	
}
