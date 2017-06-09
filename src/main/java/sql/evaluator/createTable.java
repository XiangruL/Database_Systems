package sql.evaluator;

import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class createTable {
	/* key -- column name 
	 * value -- column_number,column_type
	 */
	protected static HashMap<String, String> tableInfo = new HashMap<String, String>();
	
	public static void processSQL(CreateTable createTable) {
		List<ColumnDefinition> columnDefinitions = createTable.getColumnDefinitions();
		int colNum = 1;
		for (ColumnDefinition o :columnDefinitions) {
			StringBuffer s = new StringBuffer();
			s.append(colNum);
			s.append(",");
			s.append(o.getColDataType().toString());
			tableInfo.put(o.getColumnName(), s.toString());
			colNum++;
		}
	}
}
