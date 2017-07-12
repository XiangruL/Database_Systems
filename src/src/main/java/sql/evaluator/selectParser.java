package sql.evaluator;

import java.util.List;

import net.sf.jsqlparser.statement.select.SelectItem;

public class selectParser {
	
	public static void parseSelect(List<SelectItem> selectItemList) {
//		StringBuilder select = new StringBuilder();
//		StringBuilder project = new StringBuilder();
		for (SelectItem o : selectItemList) {
			// select *
			if (parseSelect.aliasFlag == true) {
				StringBuilder temp = new StringBuilder();
				temp = parseSelect.aliasToName(o, temp);
				parseSelect.projectTable.add(temp.toString());
			} else {
				if (o.toString().contains("*")) {
					if (createTable.allTable.get((o.toString().toLowerCase().split("\\."))[0]) != null) {
						// fullname.*
						for (String s : createTable.allTable.get((o.toString().toLowerCase().split("\\."))[0]).keySet()) {
							parseSelect.projectTable.add(s);
						}
						// *
					} else {
						for (String s : createTable.allTable.get(parseSelect.fromTable.get(0).toString().toLowerCase()).keySet()) {
							parseSelect.projectTable.add(s);
						}
					}
				} else {
					parseSelect.projectTable.add(o.toString().toLowerCase());
				}
			}
		}
//		select.append("SELECTION: ");
//		select.append(projectTable.toString().substring(1, projectTable.toString().length() - 1));
//		project.append("PROJECTION: ");
//		project.append(projectTable.toString().substring(1, projectTable.toString().length() - 1));
//		select.append("\n");
//		project.append("\n");
	}

}
