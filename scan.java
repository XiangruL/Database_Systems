package sql.evaluator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class scan {
	int lineNum = 1;

	public scan() {
	}
	
	public StringBuilder scanFile(String tableName){
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
					sb.append("|"+tableName);
				}
			}
			lineNum++;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		return sb;
	}

	public int getLineNum() {
		return lineNum;
	}

	public void setLineNum(int lineNum) {
		this.lineNum = lineNum;
	}

	@Override
	public String toString() {
		return "scan [lineNum=" + lineNum + "]";
	}
	
	
}
