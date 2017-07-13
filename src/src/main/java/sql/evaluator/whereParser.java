package sql.evaluator;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubSelect;

public class whereParser {
	
	public static void parseWhere(parseSelect p, Expression whereEx, List<Expression> whereList) {
		/** get where **/
		StringBuilder where = new StringBuilder();
		// where contains and
		if (whereEx instanceof AndExpression) {
			whereList = parseSelect.visit((AndExpression) whereEx);
			for (Expression e : whereList) {		
				if (e instanceof EqualsTo) {
					whereParser.parseEqualTo(p, e, p.aliasFlag);
				} else {
					p.condition.add(e);
				}
				where.append(e.toString().toLowerCase());
				where.append("\n");
			}
		// there is a single where statement
		} else {
			if (whereEx instanceof EqualsTo) {
				whereParser.parseEqualTo(p, whereEx, p.aliasFlag);
			} else {
				if (whereEx != null) {
					where.append(whereEx.toString().toLowerCase());
					p.condition.add(whereEx);
				} else {
					// no where statement
					where.append(whereEx);
				}
			}
		}
		
		StringBuilder join = new StringBuilder();
		join.append("JOIN: ");
		join.append(p.joins.toString().substring(1, p.joins.toString().length() - 1));
		join.append("\n");
		p.res.add(3, join);
		
		for (Expression e : p.condition) {
			p.whereCondition.add(whereParser.parseWhereCondition(p, e));
		}

	}
	
	/** Parse where expression containing "="
	 * tell join condition and select condition apart
	 * @param e - where expression
	 * @param aliasFlag - true means alias is used
	 * @param join - if there is a join condition, it needs to put it into join stringbuilder
	 * @return new updated join stringbuilder
	 */
	public static void parseEqualTo(parseSelect p,Expression e, boolean aliasFlag) {
		// get right expression 
		StringBuilder tempWhere = new StringBuilder();
		StringBuilder tempWhereL = new StringBuilder();
		StringBuilder tempWhereR = new StringBuilder();
		Expression rightE = ((EqualsTo) e).getRightExpression();
		Expression leftE = ((EqualsTo) e).getLeftExpression();
		if (aliasFlag == true) {
			// can find alias-name pair
			if (p.aliasNameMap.get((rightE.toString().toLowerCase().split("\\."))[0]) != null) {
				tempWhereL = p.aliasToName(leftE.toString(), tempWhereL);		
				tempWhereR = p.aliasToName(rightE.toString(), tempWhereR);
				tempWhere.append(tempWhereL);
				tempWhere.append(" = ");
				tempWhere.append(tempWhereR);
				
				p.joinTable.add(tempWhere.toString().toLowerCase());

				// left expression doesn't show in join yet
				if (!p.joins.toString().contains(tempWhereL.toString())) {
					p.joins.add(tempWhereL.toString());
				}
				// right expression doesn't show in join yet
				if (!p.joins.contains(tempWhereR.toString())) {
					p.joins.add(tempWhereL.toString());
				}
			} else {
				// it's selection condition not for joining			
				p.condition.add(e);
			}
		} else {
			// right expression contains ".", means it's join condition
			String temp = rightE.toString().toLowerCase();
			String temp2 = leftE.toString().toLowerCase();
			if (createTable.allTable.get((temp.split("\\."))[0]) != null) {
				p.joinTable.add(e.toString().toLowerCase());
				// left expression doesn't show in join yet
				if (!p.joins.toString().contains(temp)) {
					p.joins.add(temp);
				}
				// right expression doesn't show in join yet
				if (!p.joins.toString().contains(temp2)) {
					p.joins.add(temp2);
				}
			} else {
				p.condition.add(e);
			}
		}
	}
	
	/** parse where condition
	 * if there is alias in the left expression, change it to full name 
	 * 
	 * @param s - where condition, eg "T.name = 1"
	 * @return - return full name with condition, eg "table.name = 1"
	 */
	public static String parseWhereCondition(parseSelect p,Expression s) {
		if (s instanceof EqualsTo) {
			Expression rightE = ((EqualsTo) s).getRightExpression();
			Expression leftE = ((EqualsTo) s).getLeftExpression();
			StringBuilder temp = new StringBuilder();
			if (leftE.toString().contains("*")) {
				temp.append(leftE.toString());
			} else {
				temp = p.aliasToName(leftE, temp);
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
				temp = p.aliasToName(leftE, temp);
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
				temp = p.aliasToName(leftE, temp);
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
				temp = p.aliasToName(leftE, temp);
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
				temp = p.aliasToName(leftE, temp);
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
				temp = p.aliasToName(leftE, temp);
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
				temp = p.aliasToName(leftE, temp);
			}				
			temp.append(" != ");
			temp.append(rightE.toString());
			return temp.toString();	
		}  else if (s instanceof InExpression) {
			Expression leftE = ((InExpression) s).getLeftExpression();
			StringBuilder temp = new StringBuilder();
			temp = p.aliasToName(leftE, temp);
			temp.append(" in");
			p.subQueryTable.add((SelectBody) ((SubSelect) ((InExpression) s).getRightItemsList()).getSelectBody());
			return temp.toString();	
		} else if (s instanceof Parenthesis) {
			StringBuilder temp = new StringBuilder();

			return temp.toString();	
		}
		return s.toString();
	}

}
