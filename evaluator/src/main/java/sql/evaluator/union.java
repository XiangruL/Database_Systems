package sql.evaluator;

import java.util.ArrayList;
import java.util.HashMap;

public class union {

	public static ArrayList<StringBuilder> unionTable(ArrayList<StringBuilder> result1, 
			ArrayList<StringBuilder> result2){

		HashMap<Integer, String> result = new HashMap<>();

		for (int i = 0; i < result1.size(); i++) {
			if(!result.containsValue(result1.get(i).toString())){
				result.put(i, result1.get(i).toString());
			}else{
				result1.remove(i);
				i--;
			}
		}
		for (int i = 0; i < result2.size(); i++) {
			if(!result.containsValue(result2.get(i).toString())){
				result1.add(result2.get(i));
			}
			result2.remove(i);
			i--;
		}
		return result1;
	}

}
