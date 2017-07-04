package ParseSqlQuery;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.io.File;
import java.io.FileReader;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import sql.evaluator.createTable;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.*;

public class evel {



   public static void main(String args[]) throws ParseException, IOException {
	   InputStream inputFile = new FileInputStream(new File("C:/Users/Dan/workspace/562/src/schema.sql"));
			CCJSqlParser parser = new CCJSqlParser(inputFile);
			Statement statement;
			while((statement = parser.Statement()) != null){
		
			  if(statement instanceof Select) {
			  	Select selectStatement = (Select)statement;
			  	// handle the select
			  	////System.out.println("This is a Select stat");
			  	
			  	SelectBody selectBody = ((Select) statement).getSelectBody();
			  	////System.out.println( selectBody);
			  	
			  	
			  	//SelectItem selectItem = 
			  	
			  	
			  	
			  	
			  	//List<SelectItem> selectItem = ((PlainSelect) statement).getSelectItems();
			  //	////System.out.println( selectItem);
		
				} else if(statement instanceof CreateTable) {
			  	// and so forth
					////System.out.println("This is a CreateTable stat");
			   }
			}
			// End-of-file.  Exit!
			
			
			
			
			
			
			
			
      // create array list for six attributes
      ArrayList <String> projection = new ArrayList();
      ArrayList <String>from = new ArrayList();
      ArrayList <String>selection = new ArrayList();
      ArrayList <String>join = new ArrayList();
      ArrayList <String>groupby = new ArrayList();
      ArrayList <String>orderby = new ArrayList();
      
      
      projection.add("R.A");
      projection.add("R.B");
      projection.add("R.C");
      
      from.add("R");
      from.add("S");
      
      selection.add("R.A");
      selection.add("R.B");
      
      
//      ////System.out.println("Initial size of al: " + al.size());
//
//      // add elements to the array list
//      al.add("C");
//      al.add("A");
//      al.add("E");
//      al.add("B");
//      al.add("D");
//      al.add("F");
//      al.add(1, "A2");
      //////System.out.println("Size of al after additions: " + projection.size());

      // display the array list
      String projectionTemp = "PROJECTION: ";
      
      for (int i =0 ; i <projection.size();i++) 
      {
    	  
    	  projectionTemp = projectionTemp + projection.get(i);
    	  if (i != projection.size()-1){
    		  projectionTemp=projectionTemp+ ", ";
    	  }
      }
      //-----
      String projectionTemp2 = "FROM: ";
      
      for (int i =0 ; i <from.size();i++) 
      {
    	  
    	  projectionTemp2 = projectionTemp2 + from.get(i);
    	  if (i != from.size()-1){
    		  projectionTemp2=projectionTemp2+ ", ";
    	  }
      }
      //--------
      String projectionTemp3 = "SELECTION: ";
      
      for (int i =0 ; i <selection.size();i++) 
      {
    	  
    	  projectionTemp3 = projectionTemp3 + selection.get(i);
    	  if (i != selection.size()-1){
    		  projectionTemp3=projectionTemp3+ ", ";
    	  }
      }
      
      
      
      
      
      
      
      
      
      
      
      //Print all six attribute, add null object into Arraylist if nothing was added.
      if (projection.isEmpty()){
    	  projection.add(null);
    	  ////System.out.println("PROJECTION: null");
      }
      else {
    	  ////System.out.println(projectionTemp);
      }
      
      if (from.isEmpty()){
    	  from.add(null);
    	  ////System.out.println("FROM: null");
      }
      else {
    	  ////System.out.println(projectionTemp2);
      }
      if (selection.isEmpty()){
    	  selection.add(null);
    	  ////System.out.println("SELECTION: null");
      }
      else {
    	  ////System.out.println(projectionTemp3);
      }
      if (join.isEmpty()){
    	  join.add(null);
    	  ////System.out.println("JOIN: null");
      }
      else {
    	  ////System.out.println(projectionTemp);
      }
      if (groupby.isEmpty()){
    	  groupby.add(null);
    	  ////System.out.println("GROUP-BY: null");
    	  //ziji xie!!!
      }
      else {
    	  ////System.out.println(projectionTemp);
      }
      if (orderby.isEmpty()){
    	  orderby.add(null);
    	  ////System.out.println("ORDER-BY: null");
      }
      else {
    	  ////System.out.println(projectionTemp);
      }
//      crossProduct cro = new crossProduct();
//      ArrayList  output = new ArrayList();
//      output=cro.croPro("author", "authorship");
//      for (Object i:output){
//    	  ////System.out.println(i);
//      }
      
    //limit Test start 
    		File table1 = new File("C:/Users/Dan/workspace/562/src/publication.csv");
    		
    		FileInputStream fr1 = new FileInputStream(table1);

    		BufferedReader br1 = new BufferedReader(new InputStreamReader(fr1));
    		
    		String temp3;
    		StringBuilder mysb = new StringBuilder();
    		ArrayList<String> temp2 =new ArrayList<String>()  ;
    		ArrayList<StringBuilder> sb = new ArrayList<StringBuilder>(999);
    		int wtf2 = 0;
 
    		while((temp3= br1.readLine())!=null){
    			temp3  = temp3.replaceAll("ï»¿", "");
//    			mysb.append(temp3);
//    			sb.add(mysb);

    			//mysb.delete(0, mysb.length());
    			temp2.add(temp3);
    			
    			
    				
    		}
    		//temp2.add(temp1);
    		//System.out.println(temp2);
    		
    		for (int i= 0 ; i <temp2.size();i++){
    			StringBuilder sbsb = new StringBuilder(temp2.get(i));
    			sb.add(sbsb);
    			
    		}
    		
    		
   		
    		
//    		ArrayList<StringBuilder> sb = new ArrayList<StringBuilder>(temp2.size());
//    		StringBuilder sb2 = new StringBuilder(temp2.size());
//    		
//    		for (int i= 0 ; i <temp2.size();i++){
//    		sb2.append(temp2.get(i));
//    		sb.add(sb2);
//    		}
//    		
//    		//System.out.println(sb2);
//    		System.out.println(sb.get(0));
//     //end of test.
//      
//      
//      
//      
//      
//      
//      
//      
//    	ArrayList<String> testLim =new ArrayList<String>()  ;
//      limit lim = new limit();
//       //limit2 lim2 = new limit2();
//      // testLim= lim2.Limit(temp2, "2");
//       
//       
// 
//     // ArrayList<StringBuilder> sbOut = limit.Limit(sb, "2");
//      
//      Orderby cro = new Orderby();
//      ArrayList  output = new ArrayList();
//      output=cro.orderBy("publication.csv", "pubTypeID", 0,null,"Int");
//       
//      for (Object i:output){
//      	  System.out.println(i);
//        }
      //for (Object i:sbOut){
    //	  System.out.println(i);
     // }
      
    		
    		
    		//score DESC
    		//score ASC
    		//P.pubTypeID
    		//P.pubTypeID DESC
    		//P.pubTypeID ASC
    		//P.DESC DESC
    		//P.ASC ASC
    		
    		
    		
    		//TESTING SCH
    		HashMap<String, HashMap<String, String>> schema = new HashMap<>();
    		HashMap<String, String> tableMap1 = new HashMap<>();
    		HashMap<String, String> tableMap2 = new HashMap<>();
    		HashMap<String, String> tableMap3 = new HashMap<>();
    		HashMap<String, String> tableMap4 = new HashMap<>();
    		HashMap<String, String> tableMap5 = new HashMap<>();
    		HashMap<String, String> tableMap6 = new HashMap<>();
    		HashMap<String, String> tableMap7 = new HashMap<>();
    		
    		
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
    		
    		tableMap5.put("authorid", "0,integer");
    		tableMap5.put("firstname", "1,varchar(50)");
    		tableMap5.put("lastname", "2,varchar(50)");
    		tableMap5.put("instituteid", "3,integer");
    		
    		tableMap6.put("authorid", "0,integer");
    		tableMap6.put("publicationid", "1,integer");
    		
    		tableMap7.put("instituteid", "0,integer");
    		tableMap7.put("iname", "1,varchar(200)");
    		tableMap7.put("yearfounded", "2,varchar(4)");
    		
    		schema.put("publication", tableMap1);
    		schema.put("publicationtype", tableMap2);
    		schema.put("venuetype", tableMap3);
    		schema.put("venue", tableMap4);
    		schema.put("author", tableMap5);
    		schema.put("authorship", tableMap6);
    		schema.put("institute", tableMap7);
    		
    		
    		//
    		
    		
    		ArrayList<StringBuilder> myInput = new ArrayList<StringBuilder>()  ;
    		ArrayList<StringBuilder> testing = new ArrayList<StringBuilder>()  ;
    		ArrayList<StringBuilder> testing2 = new ArrayList<StringBuilder>()  ;
    		ArrayList<StringBuilder> testing3 = new ArrayList<StringBuilder>()  ;
    		ArrayList<StringBuilder> testing4 = new ArrayList<StringBuilder>()  ;
    		ArrayList<StringBuilder> testing5 = new ArrayList<StringBuilder>()  ;
    		ArrayList<StringBuilder> testing6 = new ArrayList<StringBuilder>()  ;
    		ArrayList<StringBuilder> testing7 = new ArrayList<StringBuilder>()  ;
    		ArrayList<StringBuilder> testing8 = new ArrayList<StringBuilder>()  ;
    		ArrayList<StringBuilder> testing9 = new ArrayList<StringBuilder>()  ;
    		ArrayList<StringBuilder> testing10 = new ArrayList<StringBuilder>()  ;
    		ArrayList<StringBuilder> testing11 = new ArrayList<StringBuilder>()  ;
    		
    		//HashMap<String, HashMap<String, String>> allTable = new HashMap<String, HashMap<String, String>>();
    		HashMap<String, HashMap<String, String>>  allTable = createTable.allTable;
//    		HashMap<String, String> innerHashMap = new HashMap<String, String>();
//    		innerHashMap.put("wtf2", "wtf3");
//    		allTable.put("wtf", innerHashMap);
    		
//    		System.out.println(innerHashMap);
//    		System.out.println(allTable);
    		String query= "P.pubTypeID ";
 		    String query1="P.pubTypeID DESC";
    		String query2="P.DESC";
    		String query3="P.ASC";
    		String query4="P.DESC DESC";
    		String query5="P.ASC ASC";
    		String query6="P.DESC ASC";
    		
    		String query7="score DESC";
    		String query8="score ASC";
    		String query9="score";
    		String query10="DESC";
    		String query11="ASC";
    	String query12 = "count(author.instituteid)"; 
    	String query13 = "count(instituteid)";
    	String query14 = "P.title";
    	String query15 = "title";
    	String agg1 =  "author.instituteid" ;
    	String agg2 =  "max(author.instituteid)" ;
    	String query16 = "author.instituteid desc";
    	String gquery16 = "author.instituteid";
    		

    		
    		StringBuilder singleRecord = new StringBuilder() ;
    		StringBuilder singleRecord1 = new StringBuilder() ;
    		StringBuilder singleRecord2 = new StringBuilder() ;
    		StringBuilder singleRecord3 = new StringBuilder() ;
    		StringBuilder singleRecord4 = new StringBuilder() ;
    		StringBuilder singleRecord5 = new StringBuilder() ;
    		StringBuilder singleRecord6 = new StringBuilder() ;
    		StringBuilder singleRecord7 = new StringBuilder() ;
    		StringBuilder singleRecord8 = new StringBuilder() ;
    		StringBuilder singleRecord9 = new StringBuilder() ;
    		StringBuilder singleRecord10 = new StringBuilder() ;
    		
    		singleRecord.append ("-33|-0.3|Aeta Probabilistic Databases: A Scalable Approach to Belief Updating and Parameter Learning|10|2017-04-01");
    		singleRecord1.append("1|0.2|Bummarizing Large Query Logs in Ettu|10|2016-06-01");
    		singleRecord2.append("-2|-0.3|Che Exception That Improves The Rule|10|2016-03-01");
    		singleRecord3.append("-33|1.6|Provenance-aware Versioned Dataworkspaces|13|2016-08-18");
    		singleRecord4.append("-14|1.9|Small Data|13|2017-01-05");
    		singleRecord5.append("-33|1.8|Aeta Probabilistic Databases: A Scalable Approach to Belief Updating and Parameter Learning|6|2017-03-01");
    		singleRecord6.append("-33|1.1|Stopa the Truthiness and Just Be Wrong|6|2016-03-01");
    		singleRecord7.append("-33|1.2|Stopa the Truthiness and Just Be Wrong|6|2017-03-10");
    		singleRecord8.append("-3|-3.1|Stopss the Truthiness and Just Be Wrong|6|2017-03-10");
    		singleRecord9.append("-3|-2.3|Stops the Truthiness and Just Be Wrong|6|2017-03-10");
    		singleRecord10.append("-3|-3.2|Stopssa the Truthiness and Just Be Wrong|6|2017-03-10");
//    		testing.add(singleRecord);
//    		testing.add(singleRecord1);
//    		testing.add(singleRecord2);
    		//System.out.println(testing);
      Orderby2 o = new Orderby2();
//      Orderby2.orderBy2(myInput, allTable, query, singleRecord);
//      Orderby2.orderBy2(myInput, allTable, query1, singleRecord1);
//      Orderby2.orderBy2(myInput, allTable, query2, singleRecord2);
//      Orderby2.orderBy2(myInput, allTable, query3, singleRecord3);
//      Orderby2.orderBy2(myInput, allTable, query4, singleRecord4);
//      Orderby2.orderBy2(myInput, allTable, query5, singleRecord5);
//      
//      Orderby2.orderBy2(myInput, allTable, query6, singleRecord);
//      Orderby2.orderBy2(myInput, allTable, query7, singleRecord);
//      Orderby2.orderBy2(myInput, allTable, query8, singleRecord);
//      Orderby2.orderBy2(myInput, allTable, query9, singleRecord);
//      Orderby2.orderBy2(myInput, allTable, query10, singleRecord);
//      Orderby2.orderBy2(myInput, allTable, query11, singleRecord);
      //  testing = Orderby2.orderBy2(testing, allTable, query11, singleRecord);
        testing= Orderby2.orderBy2g(myInput, schema, query16, singleRecord,gquery16);//C
        testing2 = Orderby2.orderBy2g(testing,schema, query16, singleRecord1,gquery16);//A
        testing3 = Orderby2.orderBy2g(testing2, schema, query16, singleRecord2,gquery16);//B
        
        testing4 = Orderby2.orderBy2g(testing3, schema, query16, singleRecord3,gquery16);//s
        testing5 = Orderby2.orderBy2g(testing4, schema, query16, singleRecord4,gquery16);
        testing6 = Orderby2.orderBy2g(testing5, schema, query16, singleRecord5,gquery16);
        testing7 = Orderby2.orderBy2g(testing6, schema, query16, singleRecord6,gquery16);
        testing8 = Orderby2.orderBy2g(testing7, schema, query16, singleRecord7,gquery16);
        testing9 = Orderby2.orderBy2g(testing8, schema, query16, singleRecord8,gquery16);
        testing10 = Orderby2.orderBy2g(testing9, schema, query16, singleRecord9,gquery16);
        testing11 = Orderby2.orderBy2g(testing10,schema, query16, singleRecord10,gquery16);
//        
//////        System.out.println(testing3);
//        for (StringBuilder i :testing11) {
//        	System.out.println(i);//This should return the whole table with sorted attributes ASC or DESC.
//        }
//   //  testing= Orderby2.orderBy2( Orderby2.orderBy2(Orderby2.orderBy2(myInput, allTable, query, singleRecord), allTable, query1, singleRecord1), allTable, query2, singleRecord2);
//     
        
        System.out.println("Finally!!!!" );

        System.out.println("\n");
     for (StringBuilder i: testing11){
    	 System.out.println(i);
     }
//        
////        
////        ArrayList<StringBuilder> aggTable = new ArrayList<StringBuilder>()  ;
//////String query12 = "count(author.instituteid)"; 
//////String query13 = "count(instituteid)";
////String query14 = "P.title";
////String query15 = "title";
//String agg1 =  "max(author.instituteid)" ;
//String agg2 =  "max(author.instituteid)" ;
////
////      
//      aggregation a = new aggregation();
//     // System.out.println(schema);
//      aggregation.aggRow(schema,testing11,agg1);
        
        
        
        ArrayList<StringBuilder> fakeInputGroup = new ArrayList<StringBuilder>()  ;
        
        
        StringBuilder asingleRecord = new StringBuilder() ;
		StringBuilder asingleRecord1 = new StringBuilder() ;
		StringBuilder asingleRecord2 = new StringBuilder() ;
		StringBuilder asingleRecord3 = new StringBuilder() ;
		StringBuilder asingleRecord4 = new StringBuilder() ;
		StringBuilder asingleRecord5 = new StringBuilder() ;
		StringBuilder asingleRecord6 = new StringBuilder() ;
		StringBuilder asingleRecord7 = new StringBuilder() ;
		StringBuilder asingleRecord8 = new StringBuilder() ;
		StringBuilder asingleRecord9 = new StringBuilder() ;
		StringBuilder asingleRecord10 = new StringBuilder() ;
		
		asingleRecord.append ("-33|-0.3|Aeta Probabilistic Databases: A Scalable Approach to Belief Updating and Parameter Learning|0|2017-04-01");
		asingleRecord1.append("-33|-0.3|Aeta Probabilistic Databases: A Scalable Approach to Belief Updating and Parameter Learning|0|2017-04-01");
		asingleRecord2.append("-33|-0.3|Aeta Probabilistic Databases: A Scalable Approach to Belief Updating and Parameter Learning|0|2017-04-01");
		asingleRecord3.append("-33|1.6|Provenance-aware Versioned Dataworkspaces|13|2016-08-18");
		asingleRecord4.append("-14|1.9|Small Data|4|2017-01-05");
		asingleRecord5.append("-14|1.8|Aeta Probabilistic Databases: A Scalable Approach to Belief Updating and Parameter Learning|6|2017-03-01");
		asingleRecord6.append("-14|1.1|Stopa the Truthiness and Just Be Wrong|6|2016-03-01");
		asingleRecord7.append("-14|1.2|Stopa the Truthiness and Just Be Wrong|6|2017-03-10");
		asingleRecord8.append("-2|-3.1|Stopss the Truthiness and Just Be Wrong|6|2017-03-10");
		asingleRecord9.append("-2|-2.3|Stops the Truthiness and Just Be Wrong|6|2017-03-10");
		asingleRecord10.append("-2|-3.2|Stopssa the Truthiness and Just Be Wrong|6|2017-03-10");
		fakeInputGroup.add(asingleRecord);
		fakeInputGroup.add(asingleRecord1);
		fakeInputGroup.add(asingleRecord2);
		fakeInputGroup.add(asingleRecord3);
		fakeInputGroup.add(asingleRecord4);
		fakeInputGroup.add(asingleRecord5);
		fakeInputGroup.add(asingleRecord6);
		fakeInputGroup.add(asingleRecord7);
		fakeInputGroup.add(asingleRecord8);
		fakeInputGroup.add(asingleRecord9);
//		fakeInputGroup.add(asingleRecord1);
		fakeInputGroup.add(asingleRecord10);
        
        ArrayList<StringBuilder>ribi = new ArrayList<StringBuilder>();
        
        aggV2 aa = new aggV2();
        ribi = aggV2.aggRow(schema,fakeInputGroup,agg2,agg1);
//        System.out.println("start !!! " + "\n");
//        for (StringBuilder i :ribi){
//      	  System.out.println(i);
//        }    
        
        
        
        
        
        
        
        
        
        
        
        
        
//      ArrayList<StringBuilder>ribi = new ArrayList<StringBuilder>();
//      
//      aggV2 aa = new aggV2();
//      ribi = aggV2.aggRow(schema,testing11,agg1);
//      System.out.println("start !!! " + "\n");
//      for (StringBuilder i :ribi){
//    	  System.out.println(i);
//      }
        
        
        
        
        
        
        
        
        
      //System.out.println(schema);
//      distinct d = new distinct();
//      d.dist(allTable, testing11, query15);
      
      

   }
}
