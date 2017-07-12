package sql.evaluator;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;

public class whereParser {
	
//	public static void parseWhere(Expression whereEx, List<Expression> whereList) {
//		/** get where **/
//		StringBuilder where = new StringBuilder();
//		// where contains and
//		if (whereEx instanceof AndExpression) {
//			whereList = parseSelect.visit((AndExpression) whereEx);
//			for (Expression e : whereList) {		
//				if (e instanceof EqualsTo) {
//					join = parseEqualTo(e, parseSelect.aliasFlag, join);
//				} else {
//					if (!subQueryFlag) {
//						condition.add(e);
//					} else {
//						subCondition.add(e);
//					}
//				}
//				where.append(e.toString().toLowerCase());
//				where.append("\n");
//			}
//		// there is a single where statement
//		} else {
//			if (whereEx instanceof EqualsTo) {
//				join = parseEqualTo(whereEx, aliasFlag, join, subQueryFlag,subCondition, subJoinTable);
//			} else {
//				if (whereEx != null) {
//					where.append(whereEx.toString().toLowerCase());
//					if (!subQueryFlag) {
//						condition.add(whereEx);
//					} else {
//						subCondition.add(whereEx);
//					}
//				} else {
//					// no where statement
//					where.append(whereEx);
//				}
//			}
//		}
//			
//		if (!subQueryFlag) {
//			for (Expression e : condition) {
//
//				whereCondition.add(parseWhereCondition(e));
//			}
//		} else {
//			for (Expression e : subCondition) {
//
//				subWhereCondition.add(parseWhereCondition(e));
//			}
//		}
//	}
//	
//	
//	/** Parse where expression containing "="
//	 * tell join condition and select condition apart
//	 * @param e - where expression
//	 * @param aliasFlag - true means alias is used
//	 * @param join - if there is a join condition, it needs to put it into join stringbuilder
//	 * @return new updated join stringbuilder
//	 */
//	public static StringBuilder parseEqualTo(Expression e, boolean aliasFlag, StringBuilder join) {
//		// get right expression 
//		StringBuilder tempWhere = new StringBuilder();
//		StringBuilder tempWhereL = new StringBuilder();
//		StringBuilder tempWhereR = new StringBuilder();
//		Expression rightE = ((EqualsTo) e).getRightExpression();
//		Expression leftE = ((EqualsTo) e).getLeftExpression();
//		if (aliasFlag == true) {
//			// can find alias-name pair
//			if (parseSelect.aliasNameMap.get((rightE.toString().toLowerCase().split("\\."))[0]) != null) {
//				tempWhereL = parseSelect.aliasToName(leftE.toString(), tempWhereL);		
//				tempWhereR = parseSelect.aliasToName(rightE.toString(), tempWhereR);
//				tempWhere.append(tempWhereL);
//				tempWhere.append(" = ");
//				tempWhere.append(tempWhereR);
//				
//				parseSelect.joinTable.add(tempWhere.toString().toLowerCase());
//
//				// left expression doesn't show in join yet
//				if (!join.toString().contains(tempWhereL.toString())) {
//					join.append(tempWhereL);
//					join.append(", ");
//				}
//				// right expression doesn't show in join yet
//				if (!join.toString().contains(tempWhereR.toString())) {
//					join.append(tempWhereR);
//					join.append(", ");
//				}
//			} else {
//				// it's selection condition not for joining			
//				parseSelect.condition.add(e);
//			}
//		} else {
//			// right expression contains ".", means it's join condition
//			String temp = rightE.toString().toLowerCase();
//			String temp2 = leftE.toString().toLowerCase();
//			if (createTable.allTable.get((temp.split("\\."))[0]) != null) {
//				parseSelect.joinTable.add(e.toString().toLowerCase());
//				// left expression doesn't show in join yet
//				if (!join.toString().contains(temp)) {
//					join.append(temp);
//					join.append(", ");
//				}
//				// right expression doesn't show in join yet
//				if (!join.toString().contains(temp2)) {
//					join.append(temp2);
//					join.append(", ");
//				}
//			} else {
//				parseSelect.condition.add(e);
//			}
//		}
//		return join;
//	}

}
