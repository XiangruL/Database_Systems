package sql.evaluator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

public class scan {
	private boolean flag = false;
	private String tableName;
	private String tempString = null;
	private String path = main.path;
	private int totalLineNum;
	private long chars=0;//chars指的是字符数 
	public scan(String tableName) {
		this.totalLineNum = getFileLine(tableName);
		this.tableName = tableName;
	}

	public StringBuilder scanFile(){
		StringBuilder sb = new StringBuilder();
		String fileName = path+tableName+".csv";
        File file = new File(fileName);  
          
        RandomAccessFile rf = null;   
        
        try {
			rf = new RandomAccessFile(fileName, "r");
	        rf.seek(chars);   
	        
	        tempString = rf.readLine();
	        chars = rf.getFilePointer(); 
	        sb.append(tempString);
	        tempString = rf.readLine();
	        if(tempString == null){
	        	flag = true;
				chars = 0;
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

	
	public int getTotalLineNum() {
		return totalLineNum;
	}

	public void setTotalLineNum(int totalLineNum) {
		this.totalLineNum = totalLineNum;
	}

	public boolean isFlag() {
		return flag;
	}

	public void setFlag(boolean flag) {
		this.flag = flag;
	}	
	
}
