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
//	private String path = main.path;
	private int totalLineNum;
	BufferedReader br;
	
	public scan(String tableName) {
		this.totalLineNum = getFileLine(tableName);
		this.tableName = tableName;
 		File file = new File(main.path + tableName+".csv");
//		File file = new File(tableName+".csv");
		FileInputStream fis;
		try {
			fis = new FileInputStream(file);
			br = new BufferedReader(new InputStreamReader(fis));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public StringBuilder scanFile(){
  		StringBuilder sb = new StringBuilder();
 		String line = "";
  		try {
			line = br.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (line != null) {
			sb.append(line);
		} else {
			return null;
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
}
