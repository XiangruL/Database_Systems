package sql.evaluator;

import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class createTable {
	
	/* store all table information 
	 * key -- table name(string);
	 * value -- a hashmap contains column metadata
	 */
	protected static HashMap<String, HashMap<String, String>> allTable = new HashMap<String, HashMap<String, String>>();
	
	/* put column information into tableInfo from the given statement */
	public static void processSQL(CreateTable createTable) {
		/* key -- column name 
		 * value -- column_number,column_type
		 */
		HashMap<String, String> tableInfo = new HashMap<String, String>();

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
		allTable.put(createTable.getTable().getName(), tableInfo);
	}
}
