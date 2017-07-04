package ParseSqlQuery;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import sql.evaluator.parseSelect;
//score desc
//score ASC
//P.pubTypeID
//P.pubTypeID desc
//P.pubTypeID ASC
//P.desc desc
//P.ASC ASC

public class Orderby2 {

	public static ArrayList<StringBuilder> orderBy2(ArrayList<StringBuilder> result, HashMap<String, HashMap<String, String>> scheme,String query,StringBuilder singleRecord ){
		//Below ---Extract the TableName,ColName,ColType from tool class for later use.
		
	  
		
				String myTableName= "";
				String myColName="";
				String mySortingOrder="";
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

				int myColPos= tool.getColNum(scheme, myTableName, myColName); 
				String myColType= tool.getColType(scheme, myTableName, myColName); 

				//Above ---Extract the TableName,ColName,ColType from tool class for later use.

			
			if (result.size()==0){
				result.add(singleRecord);
				return result;
			}
				
				String[] row = singleRecord.toString().split("\\|");
		for (int i =0;i<result.size();i++){

			
			
			
			String[] tempRow = result.get(i).toString().split("\\|");

			
			if (!mySortingOrder.contains("desc")){

				if (tool.isLarge(myColType, tempRow[myColPos], row[myColPos])){

					result.add(i,singleRecord);

					  for (StringBuilder ii :result) {
				
				        }
					return result;
				}
				else{

					if (i== result.size()-1){
						result.add(singleRecord);
						return result;
					}

				
				}
			}
			
			else {

				if (tool.isLarge(myColType, tempRow[myColPos], row[myColPos])==false){

					result.add(i,singleRecord);

					  for (StringBuilder ii :result) {
				
				        }
					return result;
				}
				else{

					if (i== result.size()-1){
						result.add(singleRecord);
						return result;
					}

				
				}
			}
		
		}
		

		
		return result;

	}
	
	
	public static ArrayList<StringBuilder> orderBy2g(ArrayList<StringBuilder> result, HashMap<String, HashMap<String, String>> scheme,String query,StringBuilder singleRecord, String groupQuery ){
		//below extract group query info.
		
		//System.out.println("Result-------below ");
		//System.out.println("\n");
		   for (StringBuilder i: result){
		    	 //System.out.println(i);
		     }
		//System.out.println("\n");
		//System.out.println("Result-------above ");
		//Below ---Extract the TableName,ColName,ColType from tool class for later use.
		
		//System.out.println("accepting what :  " + singleRecord);
		String gtableName;
		String gcolName;
		if(groupQuery.contains(".")){
			String[] gqstr = groupQuery.split("\\.");
			gtableName = gqstr[0];
			gcolName = gqstr[1];
		}else{
			
			ArrayList<String> tName = parseSelect.getFromTable();
			if(tName.size()!=1){
				//////System.err.println("do not specify table name");
				return result;
			}
			gtableName = tName.get(0);
			gcolName = groupQuery;
			
		}

				int gcolNum = tool.getColNum(scheme, gtableName, gcolName);
				
				
				
				
				
				//above extract group query info.
				
				
				
				
				String myTableName= "";
				String myColName="";
				String mySortingOrder="";
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

				int myColPos= tool.getColNum(scheme, myTableName, myColName); 
				String myColType= tool.getColType(scheme, myTableName, myColName); 

				//Above ---Extract the TableName,ColName,ColType from tool class for later use.

			
			if (result.size()==0){
				result.add(singleRecord);
				return result;
			}
				
	
				
				
				
				
		
				myColPos = 0;// MUST DELETE!!!! SET IT TO DYNMAIC
				///////End of infors//////
				
				
				
				
				//System.out.println("gcol num " +gcolNum);
				
				//System.out.println("myColPos num  " +myColPos);
				
				//System.out.println("myColtype num  " +myColType);
				
				
				String[] row = singleRecord.toString().split("\\|");
				
				String [] firstRec1 = result.get(result.size()-1).toString().split("\\|");
				String record1 =firstRec1[gcolNum] ;
				
				//System.out.println("FIRST REC  " +record1);
				//System.out.println("match first?  " +result.get(0).toString().split("\\|")[gcolNum]);
				
				if (singleRecord.toString().split("\\|")[gcolNum].equals(record1)){
					////System.out.println(singleRecord + "vs " +record1);
				
		for (int i =0;i<result.size();i++){
//			//System.out.println("A");
			//System.out.println("Check BELOW");
			//System.out.println("\n");
			//System.out.println("This compare with below" +record1);
			//System.out.println("this should be changing: " +result.get(i).toString().split("\\|")[gcolNum]);
			//System.out.println("\n");
			//System.out.println("check ABOVE");
			
			//When comparing, only gcolnum same can compare.
			if (singleRecord.toString().split("\\|")[gcolNum].equals(result.get(i).toString().split("\\|")[gcolNum])){
				
			
			
//				//System.out.println("same"+ result.get(i));
//				//System.out.println("!!!!!!!!!!!This " + result.get(i).toString().split("\\|")[gcolNum] +" is equal to "+ record1 +" and incoming is "+singleRecord);
				
				
			
			
			
			
			
			String[] tempRow = result.get(i).toString().split("\\|");

			
			if (!mySortingOrder.contains("desc")){

				if (tool.isLarge(myColType, tempRow[myColPos], row[myColPos])){

					result.add(i,singleRecord);

					return result;
				}
				else{

					if (i== result.size()-1){
						result.add(singleRecord);
						return result;
					}

				
				}
			}
			
			else {

				if (tool.isLarge(myColType, tempRow[myColPos], row[myColPos])==false){

					result.add(i,singleRecord);

					return result;
				}
				else{

					if (i== result.size()-1){
						result.add(singleRecord);
						return result;
					}
				}
				
				}
			
			}
//			else{
//				
//
////				record11 = result.get(i).toString().split("\\|")[acolNum];
////				answer11 = result.get(i).toString().split("\\|")[acolNum]+ "|" + i;
////				record1 = result.get(i).toString().split("\\|")[gcolNum];
////				firstRec1 = result.get(i).toString().split("\\|");
////				record1 =firstRec1[gcolNum] ;
//				//System.out.println("\n");
////				//System.out.println("From what ? " + result.get(i).toString().split("\\|")[gcolNum]);
////				//System.out.println("to what ? " + record1);
//				//System.out.println("This is coming!!! " + singleRecord);
//				
////				result.get(i).toString().split("\\|")[gcolNum].equals(record1)
//				
//				
//				
//			}
		
		}
				}
				else{
					
				
					
					//System.out.println(singleRecord + "break hereQ!~@~@~" + record1);
					
					record1 = singleRecord.toString().split("\\|")[gcolNum];
					//System.out.println(singleRecord + "after reset" + record1);
					result.add(result.size(),singleRecord);
					
					
				}
		

		
		return result;

	
		
		
		
		
		
		
		
		
		
		
	}
	static int ordinalIndexOf(String str, String substr, int n) {
	    int pos = str.indexOf(substr);
	    while (--n > 0 && pos != -1){
	        pos = str.indexOf(substr, pos + 1);
	    }
	    return pos;
	}
}
