package ParseSqlQuery;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
//score desc
//score ASC
//P.pubTypeID
//P.pubTypeID desc
//P.pubTypeID ASC
//P.desc desc
//P.ASC ASC

public class Orderby2 {

	public static ArrayList<StringBuilder> orderBy2(ArrayList<StringBuilder> myInput, HashMap<String, HashMap<String, String>> allTable,String query,StringBuilder singleRecord ){
		ArrayList<String> WholeTable = new ArrayList<String>();
		ArrayList<String> PartialTable = new ArrayList<String>();
		ArrayList SortedPartialTable = new ArrayList();
		ArrayList<String> OutputTable = new ArrayList<String>();
		ArrayList<StringBuilder> WholeSortedTable = new ArrayList<StringBuilder>();
		System.out.println("OrderBy test output below:  " + "\n" );
		System.out.println("I get  " +singleRecord + "\n" );
		System.out.println("My Table in "
				+ " " +myInput );
		
		
		ArrayList<String> conversion = new ArrayList<String> ();
		conversion.add(singleRecord.toString() );
		conversion.add(singleRecord.toString() );
		conversion.add(singleRecord.toString() );
		conversion.add(singleRecord.toString() );
		conversion.add(singleRecord.toString() );

		int s;
		for (String i : conversion){
			s = Collections.frequency(conversion, i);
			//System.out.println("couting it :"+s);
		}
		
		
		
		
		
		
		
		
		
		
//		int myColPos;
		String myColName="";
		String myTableName="";
//		myColPos = 2;//Testing remove this later
//		String myColType = "varchar";//Testing remove this later
		String mySortingOrder="";//Testing remove this later
		
//Below ---Extract the TableName,ColName,ColType from tool class for later use.
		if (query.contains(".")){
			int DotPos = query.indexOf(".");
			myTableName = query.substring(0, DotPos);
			String QwithoutTableName = query.substring(DotPos, query.length());

			if (QwithoutTableName.contains(" ")){
				int SpacePos = QwithoutTableName.indexOf(" ");
				myColName = QwithoutTableName.substring(1,SpacePos);
			
			   mySortingOrder = QwithoutTableName.substring(SpacePos+1,QwithoutTableName.length());
	
			}
		
		else {
			myColName = QwithoutTableName.substring(1, QwithoutTableName.length());
		
		}

	}
	else {
		if (!query.contains(" ")){
			myColName = query;

		}		
		else{
			int SpacePos = query.indexOf(" ");
			myColName = query.substring(0, SpacePos);
			mySortingOrder = query.substring(SpacePos+1, query.length());			
		}	
	}
	
		//System.out.println(myTableName);
		//System.out.println(myColName);
		//System.out.println(allTable);
//		int myColPos= tool.getColNum(allTable, myTableName, myColName); //int
//		String myColType= tool.getColType(allTable, myTableName, myColName); //String //these are tools methond,NEED!!!
		int myColPos= 4;
		String myColType= "date";
		
		

//Above ---Extract the TableName,ColName,ColType from tool class for later use.
		
		myInput.add(singleRecord); //Every time add record it to the table it gives.
		for (StringBuilder i :myInput){
			WholeTable.add(i.toString());
			
		}
		
		////System.out.println(WholeTable);
		
		for (String i : WholeTable){ //Extract the attibute to be sort.
			
			
			if (myColPos == 0){
			int start = 0;
			int end = i.indexOf("|", i.indexOf("|") + myColPos-1);
			String temp = i.substring(start,end);
			PartialTable.add(temp);
		}
		
		
		else{
//			////System.out.println(myColPos);
			
		int start =	ordinalIndexOf(i,"|",myColPos)+1;
	
		int end = 	ordinalIndexOf(i,"|",myColPos+1);
		
		if (end ==-1 ){
			end = i.length();
		}

		
		
		
		String temp = i.substring(start,end);
//		////System.out.println(temp);
		PartialTable.add(temp);
			
			
		
			
			
			
			
		}		
			
		}
		
		
		

		
//		case 1 Int
		if (myColType == "integer"){
		ArrayList<Integer> arrayOfInts = new ArrayList<Integer>();
		for (String str : PartialTable) {
		   arrayOfInts.add(Integer.parseInt((String)str));
		}

		if (!mySortingOrder.contains("desc")){
			Collections.sort(arrayOfInts);
		}
		else {
			Collections.sort(arrayOfInts);
			Collections.reverse(arrayOfInts);


		
		}	
	      for (Integer i:arrayOfInts){
	    	 
	    	  SortedPartialTable.add(i);
	      }

		}
		
		//case 2 Double
		
		if (myColType == "double"){
			ArrayList<Double> arrayOfDouble = new ArrayList<Double>();
			for (String str : PartialTable) {
				arrayOfDouble.add(Double.parseDouble((String)str));
			}

			if (!mySortingOrder.contains("desc")){
				Collections.sort(arrayOfDouble);
			}
			else {
				Collections.sort(arrayOfDouble);
				Collections.reverse(arrayOfDouble);
			
			}	
		      for (Double i:arrayOfDouble){
		    	 
		    	  SortedPartialTable.add(i);
		      }

			}

		//case 3 String
		else if (myColType == "varchar"){

			if (!mySortingOrder.contains("desc")){
				Collections.sort( PartialTable);
				////System.out.println("ASC");
				
				
			}
			else {
				Collections.sort(PartialTable);
				Collections.reverse(PartialTable);
				//SortedAttribute.add("fuck");
				////System.out.println(mySortingOrder);
				//////System.out.println(SortedAttribute);
			
			}
		      for (String i:PartialTable){
		    	 // ////System.out.println(i);
		   
		    	  
		    	  SortedPartialTable.add(i);
		      }
			
			
			
			}
		//case 4 Date
			else if (myColType == "date"){
				ArrayList<Date> arrayOfDate = new ArrayList<Date>();

				
			

				for (String str : PartialTable) {

								
					
					arrayOfDate.add(java.sql.Date.valueOf(str));
				}
				
				
				
				if (!mySortingOrder.contains("desc")){
					Collections.sort(arrayOfDate);
//					////////System.out.println("sort it asc ");
//					////////System.out.println(arrayOfDate);
					
				}
				else {
					Collections.sort(arrayOfDate);
					Collections.reverse(arrayOfDate);
//////////System.out.println("wtf");
				
				}
				
			      for (Date i:arrayOfDate){
			    	 // ////////System.out.println(i);
			    	  SortedPartialTable.add(i);
			      }

				
				}
		
		
		
		//???
		for (Object i : SortedPartialTable){
			
		////System.out.println("sortedPartial" +i);

		
		}
		
		
		for (String i : WholeTable){
			
			////System.out.println("Whole" +i);

			
			}
		
		for (String i : PartialTable){
			
			////System.out.println("Partial" +i);

			
			}
		
		//Ready to output the whole sorted table now, comparing them ...
		for (int i = 0 ;i <SortedPartialTable.size();i++){
	    	for (int j = 0; j<WholeTable.size();j++){
	    		int start;
	    		int end;
	    		
				if (myColPos == 0){
					 start = 0;
					 end = WholeTable.get(j).indexOf("|", WholeTable.get(j).indexOf("|") + myColPos-1);
	
				
				}
				
				
				else{

					
					start =	ordinalIndexOf(WholeTable.get(j),"|",myColPos)+1;
			
					end = ordinalIndexOf(WholeTable.get(j),"|",myColPos+1);
				
				if (end ==-1 ){
					end = WholeTable.get(j).length();
				}
				else {
					
				}

				}

				String temp = (String.valueOf(SortedPartialTable.get(i)));
				String temp2 =WholeTable.get(j).substring(start,end);
//				//////System.out.println("temp is "+ temp);
//				//////System.out.println("temp2 is " +temp2);
	    		if (temp.equals(temp2)){
	    			 ////////System.out.println("~#dnskfbnjiwebiurgb1iu2gviuv312jkhvrjdnasvhvqweqwejkqw");
	  
	    			OutputTable.add(WholeTable.get(j));
	    			j = WholeTable.size()+1;
	    		}
	    		
	    	}
	    }
		// hahah done
	
		for (String i :OutputTable){
			////System.out.println("sortedWhole" +i);
			StringBuilder sb = new StringBuilder();
			sb.append(i);
			WholeSortedTable.add(sb);
		}
//		System.out.println("My Table out + " +myInput+ "\n" );
		System.out.println("My Table out  \n"+"\n" );
		System.out.println("Order by ColPos at " + myColPos + "|| Col type is " +myColType);
		for (StringBuilder i : WholeSortedTable) {
//			System.out.println("My Table out + " );
			System.out.println(i);
			}
		System.out.println("\n" );
		return WholeSortedTable;
		
//		
		
	}
	static int ordinalIndexOf(String str, String substr, int n) {
	    int pos = str.indexOf(substr);
	    while (--n > 0 && pos != -1){
	        pos = str.indexOf(substr, pos + 1);
	    }
	    return pos;
	}
}
