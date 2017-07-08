package sql.evaluator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;

public class Orderby {
	
	
	@SuppressWarnings("unchecked")
	public static ArrayList<String> orderBy(String tableName1, String colName1,int colPos, String condtion,String colType){
		ArrayList<String> WholeTable = new ArrayList<String>();
		ArrayList<String> OutputTable = new ArrayList<String>();
		ArrayList FinalTable = new ArrayList();

		
		File table1 = new File("C:/Users/Dan/workspace/562/src/publication.csv");

		 //System.out.println(tableName1+".csv");

		String line1 = "";
		String line2 = "";
		FileInputStream fr1;
		try {
			fr1 = new FileInputStream(table1);

			BufferedReader br1 = new BufferedReader(new InputStreamReader(fr1));
			ArrayList<String> SortedAttribute = new ArrayList<String>();
			while((line1 = br1.readLine())!=null){
			line1  = line1.replaceAll("﻿", "");
				WholeTable.add(line1);
				if (colPos == 0){
					int start = 0;
					int end = line1.indexOf("|", line1.indexOf("|") + colPos-1);
					String temp = line1.substring(start,end);
					SortedAttribute.add(temp);
				}
				
				
				else{
//					String temp = line1+"|"+line2;
					
				int start =	ordinalIndexOf(line1,"|",colPos)+1;
			
				int end = 	ordinalIndexOf(line1,"|",colPos+1);
				
				if (end ==-1 ){
					end = line1.length();
				}
//				System.out.println(start);
//				System.out.println(end);
				
				
				
				String temp = line1.substring(start,end);
				//System.out.println(temp);
				SortedAttribute.add(temp);
				}

				
			}
			//case 1 Int
			if (colType == "Int"){
			ArrayList<Integer> arrayOfInts = new ArrayList<Integer>();
			for (String str : SortedAttribute) {
			   arrayOfInts.add(Integer.parseInt((String)str));
			}

			if (condtion==null){
				Collections.sort(arrayOfInts);
			}
			else {
				Collections.sort(arrayOfInts);
				Collections.reverse(arrayOfInts);
//				System.out.println(arrayOfInts);

			
			}	
		      for (Integer i:arrayOfInts){
		    	  FinalTable.add(i);
		      }

			}
			
			//case 2 Double
			
			if (colType == "Double"){
				ArrayList<Double> arrayOfDouble = new ArrayList<Double>();
				for (String str : SortedAttribute) {
					arrayOfDouble.add(Double.parseDouble((String)str));
				}

				if (condtion==null){
					Collections.sort(arrayOfDouble);
				}
				else {
					Collections.sort(arrayOfDouble);
					Collections.reverse(arrayOfDouble);
				
				}	
			      for (Double i:arrayOfDouble){
			    	  FinalTable.add(i);
			      }

				}

			//case 3 String
			else if (colType == "String"){

				if (condtion==null){
					Collections.sort( SortedAttribute);
					
				}
				else {
					Collections.sort(SortedAttribute);
					Collections.reverse( SortedAttribute);
				
				}
			      for (String i:SortedAttribute){
			    	 // System.out.println(i);
			    	  FinalTable.add(i);
			      }
				
				
				
				}
			//case 4 Date
				else if (colType == "Date"){
					ArrayList<Date> arrayOfDate = new ArrayList<Date>();

					
				

					for (String str : SortedAttribute) {

									
						
						arrayOfDate.add(java.sql.Date.valueOf(str));
					}
					
					
					
					if (condtion==null){
						Collections.sort(arrayOfDate);
//						System.out.println("sort it asc ");
//						System.out.println(arrayOfDate);
						
					}
					else {
						Collections.sort(arrayOfDate);
						Collections.reverse(arrayOfDate);
//System.out.println("wtf");
					
					}
					
				      for (Date i:arrayOfDate){
				    	 // System.out.println(i);
				    	  FinalTable.add(i);
				      }

					
					}
			


			

			
			
			
			
		    for (int i = 0 ;i <FinalTable.size();i++){
		    	for (int j = 0; j<WholeTable.size();j++){
		    		int start;
		    		int end;
		    		
					if (colPos == 0){
						 start = 0;
						 end = WholeTable.get(j).indexOf("|", WholeTable.get(j).indexOf("|") + colPos-1);
		
					
					}
					
					
					else{

						
						start =	ordinalIndexOf(WholeTable.get(j),"|",colPos)+1;
				
						end = ordinalIndexOf(WholeTable.get(j),"|",colPos+1);
					
					if (end ==-1 ){
						end = WholeTable.get(j).length();
					}
					else {
						
					}

					}

					String temp = (String.valueOf(FinalTable.get(i)));
					String temp2 =WholeTable.get(j).substring(start,end);
//					System.out.println("temp is "+ temp);
//					System.out.println("temp2 is " +temp2);
		    		if (temp.equals(temp2)){
		    			 //System.out.println("~#dnskfbnjiwebiurgb1iu2gviuv312jkhvrjdnasvhvqweqwejkqw");
		  
		    			OutputTable.add(WholeTable.get(j));
		    			j = WholeTable.size()+1;
		    		}
		    		
		    	}
		    }
		      
		    
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		
		for (String i:OutputTable){
		 System.out.println(i);
		}
		return OutputTable;
	}
	
	
	
	static int ordinalIndexOf(String str, String substr, int n) {
	    int pos = str.indexOf(substr);
	    while (--n > 0 && pos != -1){
	        pos = str.indexOf(substr, pos + 1);
	    }
	    return pos;
	}

}
