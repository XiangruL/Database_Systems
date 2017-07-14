package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.statement.select.SelectItem;

public class selectParser {
	
	public static void parseSelect(parseSelect p, List<SelectItem> selectItemList) {
		StringBuilder project = new StringBuilder();
		for (SelectItem o : selectItemList) {
			// select *
			if (p.aliasFlag == true) {
				StringBuilder temp = new StringBuilder();
				temp = p.aliasToName(o, temp);
				p.projectTable.add(temp.toString());
			} else {
				if (o.toString().contains("*")) {
					if (createTable.allTable.get((o.toString().toLowerCase().split("\\."))[0]) != null) {
						// fullname.*
						for (String s : createTable.allTable.get((o.toString().toLowerCase().split("\\."))[0]).keySet()) {
							p.projectTable.add(s);
						}
						// *
					} else {
						for (String s : createTable.allTable.get(p.fromTable.get(0).toString().toLowerCase()).keySet()) {
							p.projectTable.add(s);
						}
					}
				} else {
					p.projectTable.add(o.toString().toLowerCase());
				}
			}
		}
		project.append("PROJECTION: ");
		project.append(p.projectTable.toString().substring(1, p.projectTable.toString().length() - 1));
		project.append("\n");
		p.res.add(0, project);

		
	}

}
