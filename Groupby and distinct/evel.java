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
    			temp3  = temp3.replaceAll("﻿", "");
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
    		
    		singleRecord.append ("0|0|Aeta Probabilistic Databases: A Scalable Approach to Belief Updating and Parameter Learning|0|2017-04-01");
    		singleRecord1.append("1|2|Bummarizing Large Query Logs in Ettu|10|2016-06-01");
    		singleRecord2.append("2|6|Che Exception That Improves The Rule|12|2016-03-01");
    		singleRecord3.append("3|6|Provenance-aware Versioned Dataworkspaces|13|2016-08-18");
    		singleRecord4.append("4|9|Small Data|4|2017-01-05");
    		singleRecord5.append("5|8|Stops the Truthiness and Just Be Wrong|6|2017-03-10");
    		singleRecord6.append("5|1|Stopa the Truthiness and Just Be Wrong|6|2017-03-10");
    		singleRecord7.append("5|2|Stopa the Truthiness and Just Be Wrong|6|2017-03-10");
    		singleRecord8.append("5|-3|Stopss the Truthiness and Just Be Wrong|6|2017-03-10");
    		singleRecord9.append("5|-2|Stops the Truthiness and Just Be Wrong|6|2017-03-10");
    		singleRecord10.append("5|-3|Stopssa the Truthiness and Just Be Wrong|6|2017-03-10");
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
        testing= Orderby2.orderBy2(myInput, allTable, query, singleRecord2);//C
        testing2 = Orderby2.orderBy2(testing, allTable, query, singleRecord);//A
        testing3 = Orderby2.orderBy2(testing2, allTable, query, singleRecord1);//B
        testing4 = Orderby2.orderBy2(testing3, allTable, query, singleRecord5);//s
        testing5 = Orderby2.orderBy2(testing4, allTable, query, singleRecord3);
        testing6 = Orderby2.orderBy2(testing5, allTable, query, singleRecord4);
        testing7 = Orderby2.orderBy2(testing6, allTable, query, singleRecord6);
        testing8 = Orderby2.orderBy2(testing7, allTable, query, singleRecord7);
        testing9 = Orderby2.orderBy2(testing8, allTable, query, singleRecord8);
        testing10 = Orderby2.orderBy2(testing9, allTable, query, singleRecord9);
        testing11 = Orderby2.orderBy2(testing10, allTable, query, singleRecord10);
        
//        System.out.println(testing3);
        for (StringBuilder i :testing5) {
//        System.out.println(i);//This should return the whole table with sorted attributes ASC or DESC.
        }
     testing= Orderby2.orderBy2( Orderby2.orderBy2(Orderby2.orderBy2(myInput, allTable, query, singleRecord), allTable, query1, singleRecord1), allTable, query2, singleRecord2);
     
//     for (StringBuilder i: testing){
//    	 //System.out.println(i);
//     }
        
//        
//        ArrayList<StringBuilder> aggTable = new ArrayList<StringBuilder>()  ;
//String query12 = "count(author.instituteid)"; 
//String query13 = "count(instituteid)";
String query14 = "P.title";
String query15 = "title";
      
//      aggregation a = new aggregation();
     // a.aggRow(allTable,  aggTable, query12);
//      System.out.println(testing5);
      distinct d = new distinct();
      d.dist(allTable, testing11, query15);
      
      

   }
}
