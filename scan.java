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
	private long chars=0;//chars指的是字符数 
	public scan(String tableName) {
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
	        if(tempString == null){
	        	flag = true;
				chars = 0;
				tempString = "";
	        }
	        sb.append(tempString);
	        
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
          
        
		return sb;
	}

	public boolean isFlag() {
		return flag;
	}

	public void setFlag(boolean flag) {
		this.flag = flag;
	}	
	
}
