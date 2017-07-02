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
	static int ordinalIndexOf(String str, String substr, int n) {
	    int pos = str.indexOf(substr);
	    while (--n > 0 && pos != -1){
	        pos = str.indexOf(substr, pos + 1);
	    }
	    return pos;
	}
}
