package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;

public class union {
	
	public ArrayList<StringBuilder> unionTable(ArrayList<StringBuilder> result1, 
			ArrayList<StringBuilder> result2){
		
		HashMap<Integer, StringBuilder> result = new HashMap<>();
		
		for (int i = 0; i < result1.size(); i++) {
			if(!result.containsValue(result1.get(i))){
				result.put(i, result1.get(i));
			}else{
				result1.remove(i);
				i--;
			}
		}
		for (int i = 0; i < result2.size(); i++) {
			if(!result.containsValue(result2.get(i))){
				result1.add(result2.get(i));
			}
			result2.remove(i);
			i--;
		}
		return result1;
	}

}
