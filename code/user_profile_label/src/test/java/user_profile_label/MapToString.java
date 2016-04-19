package user_profile_label;

import java.util.HashMap;
import java.util.Map;

public class MapToString {
	public static void main(String[] args) {
		Map<Integer, Float> map = new HashMap<Integer, Float>();
		map.put(1, 0.1F);
		map.put(2, 0.2F);
		map.put(3, 0.3F);
		map.put(4, 0.4F);
		map.put(5, 0.5F);
		map.put(6, 0.6F);
		
		System.out.println(map.toString());
	}
}
