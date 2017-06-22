package ParseSqlQuery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

public class groupBy {
	
	
	public static HashMap<Integer, String> groupBy(HashMap<String, HashMap<String, String>> allTable,
			String tableName, String colName){
		HashMap<Integer, String> attrMap = new HashMap<>();
		int colNum = tool.getColNum(allTable, tableName, colName);
		File fr = new File(tableName+".csv");
		try{
			FileInputStream fis = new FileInputStream(fr);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line = "";
			String[] str;
			int count = 0;
			
			while((line = br.readLine())!=null){
				str = line.split("\\|");
				
				if(!attrMap.containsValue(str[colNum])){
					attrMap.put(count, str[colNum]);
					count++;
				}
			}
		}catch (FileNotFoundException e) {
			// TODO: handle exception
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		return attrMap;
	}

}
