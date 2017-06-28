package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;

import ParseSqlQuery.crossProduct;

public class test {
	
	public static void main(String[] args) {
	
		HashMap<String, HashMap<String, String>> scheme = new HashMap<>();
		HashMap<String, String> tableMap1 = new HashMap<>();
		HashMap<String, String> tableMap2 = new HashMap<>();
		HashMap<String, String> tableMap3 = new HashMap<>();
		HashMap<String, String> tableMap4 = new HashMap<>();
		tableMap1.put("publicationid", "0,integer");
		tableMap1.put("pubtypeid", "1,integer");
		tableMap1.put("title", "2,varchar(200)");
		tableMap1.put("venueid", "3,integer");
		tableMap1.put("pubdate", "4,date");
		
		tableMap2.put("pubtypeid", "0,integer");
		tableMap2.put("ptype", "1,varchar(50)");
		
		tableMap3.put("venuetypeid", "0,integer");
		tableMap3.put("vtype", "1,varchar(50)");
		
		tableMap4.put("venueid", "0,integer");
		tableMap4.put("venuetypeid", "1,integer");
		tableMap4.put("vname", "2,varchar(50)");
		tableMap4.put("eventyear", "3,varchar(4)");
		
		
		scheme.put("publication", tableMap1);
		scheme.put("publicationtype", tableMap2);
		scheme.put("venueType", tableMap3);
		scheme.put("venue", tableMap4);
		
		String tableName1 = "publication";
		String tableName2 = "publicationtype";
		scan scan1 = new scan();
		scan scan2 = new scan();
		StringBuilder row1= new StringBuilder();
		StringBuilder row2= new StringBuilder();
		row1 = scan1.scanFile(tableName1);
		row2 = scan2.scanFile(tableName2);
		String query = "pubdate > '2017-01-01'";
		select select = new select();
		row1 = select.selectRow(scheme, row1, query);
		
//		//Test Projection
//		query = "publicationid,venueid,pubdate";
//		ArrayList<StringBuilder> result = new ArrayList<>();
//		sb1.append("NO.1");
//		result.add(sb1);
//		result.add(sb);
//		projection projection = new projection();
//		result = projection.proTable(result, scheme, sb, query, 0);
//		for (StringBuilder stringBuilder : result) {
//			
//			System.out.println(stringBuilder);
//		}
		
//		ArrayList<String> joinQuery = new ArrayList<>();
//		joinQuery.add("JOIN: publication.pubtypeid = publicationtype.pubtypeid");
//		joinQuery.add("JOIN: publication.venueid = venue.venueid");
//		joinQuery.add("JOIN: venuetype.venuetypeid = venue.venuetypeid");
		join join = new join();
		row1 = join.joinRow(scheme, row1, row2, "pubtypeid", "pubtypeid");
		System.out.println(row1.toString());
		System.out.println(scheme.keySet());
		System.out.println(scheme.values());
		
	}
	

}
