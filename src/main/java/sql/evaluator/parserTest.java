package sql.evaluator;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.create.table.*;


public class parserTest {
	private static final Logger LOGGER = Logger.getLogger( parserTest.class.getName() );
	
	public static void main(String[] args) {
		
		int i;
		File dataDir = null;
		ArrayList<File> sqlFiles = new ArrayList<File>();
		
		for(i = 0; i < args.length; i++) {
			if(args[i].equals("--data")) {
				dataDir = new File(args[i+1]);
				i++;
			} else {
				sqlFiles.add(new File(args[i]));
			}
		}
	
		for (File sql : sqlFiles) {
			try {
				FileReader stream = new FileReader(sql);
				
				CCJSqlParser parser = new CCJSqlParser(stream);
				Statement stmt;
				
				//BufferedReader test = new BufferedReader(stream);
				while((stmt = parser.Statement()) != null) {
					//System.out.print("LINE: " + stmt);
					if (stmt instanceof Select) {
						LOGGER.log(Level.SEVERE, "LINE: " + ((Select) stmt).getSelectBody());
						
					} else if (stmt instanceof CreateTable) {
						CreateTable ct = (CreateTable)stmt;
						createTable.processSQL(ct);
						LOGGER.log(Level.SEVERE, "LINE: " + createTable.allTable.get("publicationType").get("pubTypeID"));
					//	LOGGER.log(Level.SEVERE, "Table name is " + ct.getTable().getName());
					}
					
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
		}
	}
	
	
}