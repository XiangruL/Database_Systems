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
	
	protected static HashMap<String, String> aliasNameMap = new HashMap<String, String>();
	protected static HashMap<String, String> subAliasNameMap = new HashMap<String, String>();

	protected static HashMap<String, HashMap<String, String>>  schema = createTable.allTable;
	/* containing join condition, "table1.PID = table2.ID" */
	protected static ArrayList<String> joinTable;
	protected static ArrayList<Expression> condition;
	protected static ArrayList<String> whereCondition;
	protected static ArrayList<String> havingCondition;
	protected static ArrayList<String> groupByTable;
	protected static ArrayList<String> orderByTable;
	protected static ArrayList<String> fromTable;
	protected static ArrayList<String> projectTable;
	protected static ArrayList<String> distinctTable;
	protected static ArrayList<SelectBody> subQueryTable;
	protected static ArrayList<StringBuilder> res;
	protected static ArrayList<StringBuilder> subRes;


	
	
	public static ArrayList<StringBuilder> splitStatement(SelectBody selectBd, boolean subQueryFlag) {
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
	
		
		HashMap<String, String> mainAliasNameMap = new HashMap<String, String>();
		HashMap<String, String> subAliasNameMap = new HashMap<String, String>();
		ArrayList<String> subSelectTable = new ArrayList<String>();
		ArrayList<String> subJoinTable = new ArrayList<String>();
		ArrayList<Expression> subCondition = new ArrayList<Expression>();
		ArrayList<String> subWhereCondition = new ArrayList<String>();
		ArrayList<String> subHavingCondition = new ArrayList<String>();
		ArrayList<String> subGroupByTable = new ArrayList<String>();
		ArrayList<String> subOrderByTable = new ArrayList<String>();
		ArrayList<String> subFromTable = new ArrayList<String>();
		ArrayList<String> subProjectTable = new ArrayList<String>();
		ArrayList<String> subDistinctTable = new ArrayList<String>();

		/** get from item **/
		StringBuilder from = new StringBuilder();
		from.append("FROM: ");
		// has alias in from statement
		if (fromItem.getAlias() != null && fromItem.getAlias().toString().length() != 0) {
			// has alias, put it in alias-name map
			aliasFlag = true;
			String temp = fromItem.toString().toLowerCase();
			aliasNameMap.put((temp.split("\\s+"))[1], (temp.split("\\s+"))[0]);
			if (!subQueryFlag) {
				mainAliasNameMap.put((temp.split("\\s+"))[1], (temp.split("\\s+"))[0]);
			} else {
				subAliasNameMap.put((temp.split("\\s+"))[1], (temp.split("\\s+"))[0]);
			}			
		} else {
			// no alias, change it to lower case and put it into from 
			if (!subQueryFlag) {
				fromTable.add(fromItem.toString().toLowerCase());
			} else {
				subFromTable.add(fromItem.toString().toLowerCase());
			}
		}
		
		
		
		/* encounter nested query */
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
					if (!subQueryFlag) {
						mainAliasNameMap.put((temp.split("\\s+"))[1], (temp.split("\\s+"))[0]);
					} else {
						subAliasNameMap.put((temp.split("\\s+"))[1], (temp.split("\\s+"))[0]);
					}
				}
			}
		}
		

		
		
		/** after having all names and alias print all from item **/
		if (!subQueryFlag) {
			for (String st : aliasNameMap.keySet()) {
				fromTable.add(aliasNameMap.get(st));
			}
		} else {
			for (String st : subAliasNameMap.keySet()) {
				subFromTable.add(aliasNameMap.get(st));
			}
		}		
		from.append(fromTable.toString().substring(1, fromTable.toString().length() - 1));
		from.append("\n");


		
		/** get select item **/
		StringBuilder select = new StringBuilder();
		StringBuilder project = new StringBuilder();
		for (SelectItem o : selectItemList) {
			// select *
			if (aliasFlag == true) {
				StringBuilder temp = new StringBuilder();
				temp = aliasToName(o, temp);
				if (!subQueryFlag) {
					projectTable.add(temp.toString());
				} else {
					subProjectTable.add(temp.toString());
				}
			} else {
				if (o.toString().contains("*")) {
					if (createTable.allTable.get((o.toString().toLowerCase().split("\\."))[0]) != null) {
						// fullname.*
						for (String s : createTable.allTable.get((o.toString().toLowerCase().split("\\."))[0]).keySet()) {
							if (!subQueryFlag) {
								projectTable.add(s);
							} else {
								subProjectTable.add(s);
							}
						}
						// *
					} else {
						for (String s : createTable.allTable.get(fromItem.toString().toLowerCase()).keySet()) {
							if (!subQueryFlag) {
								projectTable.add(s);
							} else {
								subProjectTable.add(s);
							}
						}
					}
				} else {
					if (!subQueryFlag) {
						projectTable.add(o.toString().toLowerCase());
					} else {
						subProjectTable.add(o.toString().toLowerCase());
					}
				}
			}
		}
		select.append("SELECTION: ");
		select.append(projectTable.toString().substring(1, projectTable.toString().length() - 1));
		project.append("PROJECTION: ");
		project.append(projectTable.toString().substring(1, projectTable.toString().length() - 1));
		select.append("\n");
		project.append("\n");
		
		
		/** get group-by item **/
		StringBuilder groupBy = new StringBuilder();
		groupBy.append("GROUP-BY: ");
		if (groupByItemList == null) {
			if (!subQueryFlag) {
				groupByTable.add(null);
			} else {
				subGroupByTable.add(null);
			}
		} else {
			for (Expression o : groupByItemList) {
				StringBuilder temp = new StringBuilder();
				temp = aliasToName(o, temp);
				if (!subQueryFlag) {
					groupByTable.add(temp.toString());
				} else {
					subGroupByTable.add(temp.toString());
				}
			}
		}
		groupBy.append(groupByTable.toString().substring(1, groupByTable.toString().length() - 1));
		groupBy.append("\n");
		
		
		/** get order-by item  **/
		StringBuilder orderBy = new StringBuilder();
		orderBy.append("ORDER-BY: ");
		if (orderByItemList == null) {
			if (!subQueryFlag) {
				orderByTable.add(null);
			} else {
				subOrderByTable.add(null);
			}
		} else {
			for (OrderByElement o : orderByItemList) {
				StringBuilder temp = new StringBuilder();
				temp =  aliasToName(o, temp);
				if (!subQueryFlag) {
					orderByTable.add(temp.toString());
				} else {
					subOrderByTable.add(temp.toString());
				}
			}
		}
		orderBy.append(orderByTable.toString().substring(1, orderByTable.toString().length() - 1));
		orderBy.append("\n");
		
		
		/** get where **/
		StringBuilder where = new StringBuilder();
		// where contains and
		if (whereEx instanceof AndExpression) {
			whereList = visit((AndExpression) whereEx);
			for (Expression e : whereList) {		
				if (e instanceof EqualsTo) {
					join = parseEqualTo(e, aliasFlag, join, subQueryFlag,subCondition, subJoinTable);
				} else {
					if (!subQueryFlag) {
						condition.add(e);
					} else {
						subCondition.add(e);
					}
				}
				where.append(e.toString().toLowerCase());
				where.append("\n");
			}
		// there is a single where statement
		} else {
			if (whereEx instanceof EqualsTo) {
				join = parseEqualTo(whereEx, aliasFlag, join, subQueryFlag,subCondition, subJoinTable);
			} else {
				if (whereEx != null) {
					where.append(whereEx.toString().toLowerCase());
					if (!subQueryFlag) {
						condition.add(whereEx);
					} else {
						subCondition.add(whereEx);
					}
				} else {
					// no where statement
					where.append(whereEx);
				}
			}
		}
			
		if (!subQueryFlag) {
			for (Expression e : condition) {

				whereCondition.add(parseWhereCondition(e));
			}
		} else {
			for (Expression e : subCondition) {

				subWhereCondition.add(parseWhereCondition(e));
			}
		}
				
		/** get having **/
		// having contains and
		if (havingEx != null){
			if (!subQueryFlag) {
				havingCondition.add(parseWhereCondition(havingEx));
			} else {
				subHavingCondition.add(parseWhereCondition(havingEx));
			}
		}
		
		/** get distinct **/
		if (distinctI != null) {
			if (distinctI.toString().toLowerCase().equals("distinct")) {
				if (!subQueryFlag) {
					distinctTable.add(projectTable.get(0));
				} else {
					subDistinctTable.add(subProjectTable.get(0));
				}
				
			}
		} else {
			distinctTable = null;
			if (!subQueryFlag) {
				distinctTable = null;
			} else {
				subDistinctTable = null;
			}
		}
		
		/** sort join table 
		 * This step must be placed before parseSelect.algebraGen() **/
		if (!subQueryFlag) {
			if (!parseSelect.joinTable.isEmpty()) {
				parseSelect.joinTable = parseSelect.sortJoinTable(parseSelect.joinTable);
				parseSelect.fromTable = parseSelect.sortFromTable(parseSelect.joinTable, parseSelect.fromTable);
			}
		}else {
			if (!subJoinTable.isEmpty()) {
				subJoinTable = parseSelect.sortJoinTable(subJoinTable);
				subFromTable = parseSelect.sortFromTable(subJoinTable, subFromTable);
			}
		}

		/** Print out the query evaluated result **/
		if (!subQueryFlag) {
			res.add(0, project);
			res.add(1, from);
			res.add(2, select);
			if (join.charAt(join.length() - 2) == ',')
				join.replace(join.length() - 2, join.length(), "\n");

			else  {
				join.append("\n");
			}
			res.add(3, join);
			res.add(4, groupBy);
			res.add(5, orderBy);
		} else {
			StringBuilder temp = res.get(2);
			temp.insert(temp.length() - 1, ", ");
			temp.insert(temp.length() - 1, subProjectTable.toString().substring(1, subProjectTable.toString().length() - 1));
			res.set(2, temp);
			// add subgroupby to groupby
			if (!subGroupByTable.isEmpty()) {
				temp = res.get(4);
				if (parseSelect.groupByTable.get(0) == null) {
					StringBuilder s = new StringBuilder();
					s.append("GROUP BY: ");
					s.append(subGroupByTable.toString().substring(1, subGroupByTable.toString().length() - 1));
					s.append("\n");
					res.set(4, s);
				} else {
					temp.insert(temp.length() - 1, ", ");
					temp.insert(temp.length() - 1, subGroupByTable.toString().substring(1, subGroupByTable.toString().length() - 1));
				}
			}
			if (!subOrderByTable.isEmpty()) {
				temp = res.get(5);
				if (orderByTable.get(0) == null) {
					StringBuilder s = new StringBuilder();
					s.append("ORDER BY: ");
					s.append(subOrderByTable.toString().substring(1, subOrderByTable.toString().length() - 1));
					s.append("\n");
					res.set(5, s);
				} else {
					temp.insert(temp.length() - 1, ", ");
					temp.insert(temp.length() - 1, subOrderByTable.toString().substring(1, subOrderByTable.toString().length() - 1));
				}
			}
		}
		
		if (!subQueryFlag) {
			if (!subQueryTable.isEmpty()) {
				parseSubQuery(subQueryTable.get(0));
			}
			else {
				subRes = processPlainSelect.algebraGen(parseSelect.joinTable, parseSelect.whereCondition, parseSelect.havingCondition, parseSelect.groupByTable, parseSelect.orderByTable, parseSelect.fromTable, parseSelect.projectTable, parseSelect.distinctTable, subRes);
			}
		} else {
			subRes =  processPlainSelect.algebraGen(subJoinTable, subWhereCondition, subHavingCondition, subGroupByTable, subOrderByTable, subFromTable, subProjectTable, subDistinctTable, subRes);
			subRes = processPlainSelect.algebraGen(parseSelect.joinTable, parseSelect.whereCondition, parseSelect.havingCondition, parseSelect.groupByTable, parseSelect.orderByTable, parseSelect.fromTable, parseSelect.projectTable, parseSelect.distinctTable, subRes);		
		}

		return new ArrayList<StringBuilder>();
	}
	
	/* Enter a subquery */
	public static void parseSubQuery(SelectBody selectBd) {
		splitStatement(selectBd, true);
	}
	
	/** Translate a give alias or name to a full table name
	 * 
	 * @param o - given object
	 * @param s - to append full name in this string builder
	 * @return
	 */
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
	public static StringBuilder parseEqualTo(Expression e, boolean aliasFlag, StringBuilder join, boolean subQueryFlag, ArrayList<Expression> subCondition, ArrayList<String> subJoinTable) {
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
				if (!subQueryFlag) {
					parseSelect.joinTable.add(tempWhere.toString().toLowerCase());
				} else {
					subJoinTable.add(tempWhere.toString().toLowerCase());
				}
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
				if (!subQueryFlag) {
					condition.add(e);
				} else {
					subCondition.add(e);
				}
			}
		} else {
			// right expression contains ".", means it's join condition
			String temp = rightE.toString().toLowerCase();
			String temp2 = leftE.toString().toLowerCase();
			if (createTable.allTable.get((temp.split("\\."))[0]) != null) {
				if (!subQueryFlag) {
					joinTable.add(e.toString().toLowerCase());
				} else {
					subJoinTable.add(e.toString().toLowerCase());
				}
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
				if (!subQueryFlag) {
					condition.add(e);
				} else {
					subCondition.add(e);
				}
			}
		}
		return join;
	}

	
	/** 
	 * @return fromTable containing from item
	 */
	public static ArrayList<String> getFromTable() {
		return parseSelect.fromTable;
	}
	
	
	/** This method use to join two tables, return the first row match the joining condition
	 * 
	 * @param scanner1 - scan for table 1
	 * @param scanner2 - scan for table 2
	 * @param query - join condition, ex "t1.a = t2.b"
	 * @param startFlag - true if it's first time to enter the join
	 * @return
	 */
	public static StringBuilder joinTwoTable(scan scanner1, scan scanner2, String query, boolean startFlag, HashMap<String, HashMap<String, String>> oldschema, boolean joinF, boolean updateSchema) {
		StringBuilder temp;
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
//				temp2 = join.joinRow(oldschema, createTable.allTable, null, scanner1.getLine(), scanner2.scanFile(), query, joinF, updateSchema);
				updateSchema = false;
				if (temp2 != null && temp2.length() > 0) {
					parseSelect.schema = join.getNewSchema();
					return temp2;
				}
			}
		}
		// no match at these two table, return an empty string builder
		return new StringBuilder();
	}
	
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
	public static StringBuilder joinRowAndTable(StringBuilder row, scan scanner, String tableName,String query, HashMap<String, HashMap<String, String>> oldschema, boolean joinF, boolean updateSchema) {
		boolean flag = scanner.isFlag();
		boolean schemaFlag = false;
		StringBuilder temp = new StringBuilder();
		while(!flag) {
			flag = scanner.isFlag();
			temp = join.joinRow(oldschema, createTable.allTable, tableName, row, scanner.scanFile(), query, joinF, updateSchema);
			updateSchema = false;
			if (temp != null && temp.length() > 0) {
				parseSelect.schema = join.getNewSchema();
				return temp;
			}
		}
		return temp;
	}
	
	
	/** sort join table, so it can use this table directly in joining operation
	 * 
	 * @param joinT - the complete join condition, eg, ["A.a = B.b", "B.a = C.d"]
	 */
	public static ArrayList<String> sortJoinTable(ArrayList<String> joinT) {
		ArrayList<String> newJoinTable = new ArrayList<String>();
		HashSet<String> tableName = new HashSet<String>(); 
		for (String s : joinT) {
			StringBuilder temp = new StringBuilder();
			String[] s1 = s.split(" = ");
			String[] itm1 = s1[0].split("\\.");
			String[] itm2 = s1[1].split("\\.");
			// if at least one table showed before, put this condition in the join table 
			if (tableName.size() == 0 || tableName.contains(itm1[0]) || tableName.contains(itm2[0])) {
				if (tableName.contains(itm1[0])) {
					temp.append(s1[0]);
					temp.append(" = ");
					temp.append(s1[1]);
				} else if (tableName.contains(itm2[0])) {
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
				joinT.add(s);
			}
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
			if (leftE.toString().contains("*")) {
				temp.append(leftE.toString());
			} else {
				temp = aliasToName(leftE, temp);
			}
			temp.append(" = ");
			temp.append(rightE.toString());
			return temp.toString();		
		} else if (s instanceof GreaterThan) {
			Expression rightE = ((GreaterThan) s).getRightExpression();
			Expression leftE = ((GreaterThan) s).getLeftExpression();
			StringBuilder temp = new StringBuilder();
			if (leftE.toString().contains("*")) {
				temp.append(leftE.toString());
			} else {
				temp = aliasToName(leftE, temp);
			}			
			temp.append(" > ");
			temp.append(rightE.toString());
			return temp.toString();	
		} else if (s instanceof GreaterThanEquals) {
			Expression rightE = ((GreaterThanEquals) s).getRightExpression();
			Expression leftE = ((GreaterThanEquals) s).getLeftExpression();
			StringBuilder temp = new StringBuilder();
			if (leftE.toString().contains("*")) {
				temp.append(leftE.toString());
			} else {
				temp = aliasToName(leftE, temp);
			}			
			temp.append(" >= ");
			temp.append(rightE.toString());
			return temp.toString();	
		} else if (s instanceof LikeExpression) {
			Expression rightE = ((LikeExpression) s).getRightExpression();
			Expression leftE = ((LikeExpression) s).getLeftExpression();
			StringBuilder temp = new StringBuilder();
			if (leftE.toString().contains("*")) {
				temp.append(leftE.toString());
			} else {
				temp = aliasToName(leftE, temp);
			}	
			if (s.toString().toLowerCase().contains("not like")) {
				temp.append(" notlike ");
			} else {
				temp.append(" like ");
			}
			temp.append(rightE.toString());
			return temp.toString();	
		} else if (s instanceof MinorThanEquals) {
			Expression rightE = ((MinorThanEquals) s).getRightExpression();
			Expression leftE = ((MinorThanEquals) s).getLeftExpression();
			StringBuilder temp = new StringBuilder();
			if (leftE.toString().contains("*")) {
				temp.append(leftE.toString());
			} else {
				temp = aliasToName(leftE, temp);
			}				
			temp.append(" <= ");
			temp.append(rightE.toString());
			return temp.toString();	
		} else if (s instanceof MinorThan) {
			Expression rightE = ((MinorThan) s).getRightExpression();
			Expression leftE = ((MinorThan) s).getLeftExpression();
			StringBuilder temp = new StringBuilder();
			if (leftE.toString().contains("*")) {
				temp.append(leftE.toString());
			} else {
				temp = aliasToName(leftE, temp);
			}				
			temp.append(" < ");
			temp.append(rightE.toString());
			return temp.toString();	
		} else if (s instanceof NotEqualsTo) {
			Expression rightE = ((NotEqualsTo) s).getRightExpression();
			Expression leftE = ((NotEqualsTo) s).getLeftExpression();
			StringBuilder temp = new StringBuilder();
			if (leftE.toString().contains("*")) {
				temp.append(leftE.toString());
			} else {
				temp = aliasToName(leftE, temp);
			}				
			temp.append(" != ");
			temp.append(rightE.toString());
			return temp.toString();	
		}  else if (s instanceof InExpression) {
			Expression leftE = ((InExpression) s).getLeftExpression();
			StringBuilder temp = new StringBuilder();
			temp = aliasToName(leftE, temp);
			temp.append(" in");
			subQueryTable.add((SelectBody) ((SubSelect) ((InExpression) s).getRightItemsList()).getSelectBody());
			return temp.toString();	
		} else if (s instanceof Parenthesis) {
			StringBuilder temp = new StringBuilder();

			return temp.toString();	
		}
		return s.toString();
	}
	
}
