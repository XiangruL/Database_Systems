package sql.evaluator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class scan {
	int lineNum = 1;
	int totalLineNum;
	boolean flag = false;
	String tableName;

	public scan(String tableName) {
		totalLineNum = getFileLine(tableName);
		this.tableName = tableName;
	}
	
	public StringBuilder scanFile(){
		flag = false;
		StringBuilder sb = new StringBuilder();
		String line = "";
		int count = 0;
		File file = new File(tableName+".csv");
		try {
			FileInputStream fis = new FileInputStream(file);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			while((line=br.readLine())!=null){
				
				count++;
				
				if(count==lineNum){
					sb.append(line);
				}
			}
			lineNum++;
			
			if((lineNum - totalLineNum) ==1){
				
				flag = true;
				fis.getChannel().position(0);
				lineNum = 1;
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}
		
		
		
		return sb;
	}
	
	public int getFileLine(String tableName){
		File file = new File(tableName+".csv");
		String line = "";
		int count = 0;
		try {
			FileInputStream fis = new FileInputStream(file);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			while((line=br.readLine())!=null){
				count++;
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return count;
	}

	public int getLineNum() {
		return lineNum;
	}

	public void setLineNum(int lineNum) {
		this.lineNum = lineNum;
	}
	
	

	public int getTotalLineNum() {
		return totalLineNum;
	}


	public boolean isFlag() {
		return flag;
	}

	public void setFlag(boolean flag) {
		this.flag = flag;
	}


	
	
	
}
