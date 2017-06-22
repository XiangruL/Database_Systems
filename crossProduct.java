package ParseSqlQuery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class crossProduct {
	
	public static ArrayList croPro(String tableName1, String tableName2){
		ArrayList newTable = new ArrayList();
		ArrayList tempTable = new ArrayList();
		File table1 = new File(tableName1+".csv");
		File table2 = new File(tableName2+".csv");
		String line1 = "";
		String line2 = "";
		FileInputStream fr1;
		try {
			fr1 = new FileInputStream(table1);
			FileInputStream fr2 = new FileInputStream(table2);
			BufferedReader br1 = new BufferedReader(new InputStreamReader(fr1));
			BufferedReader br2 = new BufferedReader(new InputStreamReader(fr2));
			
			
			
			
			while((line1 = br1.readLine())!=null){
				while((line2 = br2.readLine())!=null){
					String temp = line1+"|"+line2;
					String str = temp.replaceAll("\\|", ",");
					newTable.add(str);
				}
				fr2.getChannel().position(0);
						
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		return newTable;
	}

}
