package sql.evaluator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

public class scan {
	private int lineNum = 0;
	private int totalLineNum;
	private boolean flag = false;
	private String tableName;
	private String line = "";
	private long chars=0;//chars指的是字符数 
	public scan(String tableName) {
		totalLineNum = getFileLine(tableName);
		this.tableName = tableName;
	}

public StringBuilder scanFile(){
		
		StringBuilder sb = new StringBuilder();
		if(flag){
			return sb;
		}
		
		String fileName = tableName+".csv";
        File file = new File(fileName);  
          
        RandomAccessFile rf = null;  
          
        String tempString = null;  
        
        try {
			rf = new RandomAccessFile(fileName, "r");
	        rf.seek(chars);   
	        
	        tempString = rf.readLine();
	        sb.append(tempString);

	        chars = rf.getFilePointer(); 
	        lineNum++;
	        if((lineNum - totalLineNum) ==0){
				flag = true;
				chars = 0;
				lineNum = 0;
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

	public String getTableName() {
		return tableName;
	}

	public StringBuilder getLine() {
		return new StringBuilder(line);
	}	
	
}
