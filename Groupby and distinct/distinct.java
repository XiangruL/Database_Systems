package ParseSqlQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class distinct {
	public ArrayList<StringBuilder> dist(HashMap<String, HashMap<String, String>> schema,
			ArrayList<StringBuilder> result, String query){
		
		for (StringBuilder i : result) {
			System.out.println(" Before removal: " +i);
		}
		
		
		////System.out.println(MarkedDup);
		Map<Integer, Object> map = new HashMap<Integer, Object>();
		String repeat ="";
		String temp="";
		int cout = 0;

		String myTableName="";
		String myColName="";
		int myColPos=2;
		//Below ---Extract the TableName,ColName,ColType from tool class for later use.
				if (query.contains(".")){
					int DotPos = query.indexOf(".");
					myTableName = query.substring(0, DotPos);
					String QwithoutTableName = query.substring(DotPos, query.length());

					if (QwithoutTableName.contains(" ")){
						int SpacePos = QwithoutTableName.indexOf(" ");
						myColName = QwithoutTableName.substring(1,SpacePos);
					
					  
			
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
					
				}	
			}
				//above is extraction of TableName and ColName
//				myColPos = tool.getColNum(schema, myTableName, myColName);
				
				
				//System.out.println(myTableName);
				//System.out.println(myColName);
				//System.out.println(myColPos);
			System.out.println("I am removing duplicate at col AT " + myColPos );
				for (int i =0;i<result.size();i++){

					
					if (myColPos == 0){
						 int start = 0;
						 int end = result.get(i).indexOf("|", result.get(i).indexOf("|") + myColPos-1);
							//System.out.println(start);
							//System.out.println(end);
							repeat= result.get(i).toString().substring(start,end);
							//System.out.println(temp);
							if (map.containsValue(repeat)){
								
								
								
								
								System.out.println(repeat +" got removed");
								result.remove(i);
								i=i-1;
					
							}
							else {
								map.put(i, repeat);
								System.out.println(i + " was put into the map");
								
							}
					
					}
					
					
					else{


						int start =	ordinalIndexOf(result.get(i).toString(),"|",myColPos)+1;
				
						int end = ordinalIndexOf(result.get(i).toString(),"|",myColPos+1);
						
						if (end ==-1 ){
							end = result.get(i).length();
						}

						//System.out.println(start);
						//System.out.println(end);
						repeat= result.get(i).toString().substring(start,end);
						//System.out.println(repeat);
						
						if (map.containsValue(repeat)){
							
							
							
							
							System.out.println(repeat +" got removed");
							result.remove(i);
							i=i-1;
				
						}
						else {
							map.put(i, repeat);
						}
						
						//System.out.println("wtf" +repeat);
						//System.out.println(i);
						
						
						
						
						
						
						
						
					}
					
					
					
					
					cout=cout+1;
				}
				
				
			
		
		
		
		



				
				for (StringBuilder i : result) {
					//System.out.println(i);
					System.out.println(" After removal: " +i);
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