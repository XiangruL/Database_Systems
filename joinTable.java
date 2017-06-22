package ParseSqlQuery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import net.sf.jsqlparser.statement.create.table.CreateTable;

public class joinTable {
	ArrayList newTable = new ArrayList<>();
	
	public ArrayList join(HashMap<String, HashMap<String, String>> allTable,
			String tableName1, String tableName2, String colName1, 
			String colName2, String condition){
		
		//get the column number, cast to Integer;
		int colNum1 = tool.getColNum(allTable, tableName1, colName1);
		int colNum2 = tool.getColNum(allTable, tableName2, colName2);	
		
		switch (condition) {
		case "=":
			File table1 = new File(tableName1+".csv");
			File table2 = new File(tableName2+".csv");
			String line1 = "";
			String line2 = "";
			String[] str1;
			String[] str2;
			FileInputStream fr1;
			try {
				fr1 = new FileInputStream(table1);
				FileInputStream fr2 = new FileInputStream(table2);
				BufferedReader br1 = new BufferedReader(new InputStreamReader(fr1));
				BufferedReader br2 = new BufferedReader(new InputStreamReader(fr2));
				
				while((line1 = br1.readLine())!=null){
					
					str1 = line1.split("\\|");
					
					while((line2 = br2.readLine())!=null){
						str2 = line2.split("\\|");
						
						if(str1[colNum1].equals(str2[colNum2])){
							String str = "";
							for(int j = 0; j<str1.length;j++){
								str+= str1[j]+",";
							}
							for(int j = 0; j<str2.length;j++){
								if(j == colNum2){
									continue;
								}
								if(j == str2.length-1){
									str+= str2[j];
									break;
								}
								str+= str2[j]+",";
							}
							newTable.add(str);
						}
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
			break;
			
		case "null":
			newTable = crossProduct.croPro(tableName1, tableName2);
			break;

		default:
			break;
		}
		return newTable;
		
	}
	

}
