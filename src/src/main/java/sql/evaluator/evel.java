package sql.evaluator;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
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
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.*;

public class evel {



   public static void main(String args[]) throws ParseException, FileNotFoundException {
	   InputStream inputFile = new FileInputStream(new File("C:/Users/Dan/workspace/562/src/schema.sql"));
			CCJSqlParser parser = new CCJSqlParser(inputFile);
			Statement statement;
			while((statement = parser.Statement()) != null){
		
			  if(statement instanceof Select) {
			  	Select selectStatement = (Select)statement;
			  	// handle the select
			  	//system.out.println("This is a Select stat");
			  	
			  	SelectBody selectBody = ((Select) statement).getSelectBody();
			  	//system.out.println( selectBody);
			  	
			  	
			  	//SelectItem selectItem = 
			  	
			  	
			  	
			  	
			  	//List<SelectItem> selectItem = ((PlainSelect) statement).getSelectItems();
			  //	//system.out.println( selectItem);
		
				} else if(statement instanceof CreateTable) {
			  	// and so forth
					//system.out.println("This is a CreateTable stat");
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
      
      
//      //system.out.println("Initial size of al: " + al.size());
//
//      // add elements to the array list
//      al.add("C");
//      al.add("A");
//      al.add("E");
//      al.add("B");
//      al.add("D");
//      al.add("F");
//      al.add(1, "A2");
      ////system.out.println("Size of al after additions: " + projection.size());

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
    	  //system.out.println("PROJECTION: null");
      }
      else {
    	  //system.out.println(projectionTemp);
      }
      
      if (from.isEmpty()){
    	  from.add(null);
    	  //system.out.println("FROM: null");
      }
      else {
    	  //system.out.println(projectionTemp2);
      }
      if (selection.isEmpty()){
    	  selection.add(null);
    	  //system.out.println("SELECTION: null");
      }
      else {
    	  //system.out.println(projectionTemp3);
      }
      if (join.isEmpty()){
    	  join.add(null);
    	  //system.out.println("JOIN: null");
      }
      else {
    	  //system.out.println(projectionTemp);
      }
      if (groupby.isEmpty()){
    	  groupby.add(null);
    	  //system.out.println("GROUP-BY: null");
    	  //ziji xie!!!
      }
      else {
    	  //system.out.println(projectionTemp);
      }
      if (orderby.isEmpty()){
    	  orderby.add(null);
    	  //system.out.println("ORDER-BY: null");
      }
      else {
    	  //system.out.println(projectionTemp);
      }
//      crossProduct cro = new crossProduct();
//      ArrayList  output = new ArrayList();
//      output=cro.croPro("author", "authorship");
//      for (Object i:output){
//    	  //system.out.println(i);
//      }
      
      Orderby cro = new Orderby();
      ArrayList  output = new ArrayList();
      output=cro.orderBy("publication.csv", "pubTypeID", 0,null,"Int");
      for (Object i:output){
    	  ////system.out.println(i);
      }
      
      
      
      
      

   }
}
