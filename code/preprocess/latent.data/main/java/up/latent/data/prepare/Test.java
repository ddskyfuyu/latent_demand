package up.latent.data.prepare;

import java.util.Map;
import java.util.TreeMap;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Test test = new Test();
		test.testReduce();
		
	}

	public void mapTest(){
		String allMonth = "201510,201511";
		int comCateAmount = 977;
		int mediaCateAmount = 947;
		String line = "bc54ecf4bbe35d400000299c00033e2356602389#1317	#2015-12-29";
		String[] lineArray = line.split("#");

		if(lineArray.length == 3){
			
			String gid = lineArray[0].trim();
			String categoryIndex = lineArray[1].trim();
			String date = lineArray[2].trim();
			int columnIndex = -1;
			
			//防止日期格式中的异常
			if(date != null && date.length() > 0){
				String[] dateArray = date.split("-");
				if(dateArray.length == 3){
					String specificMonth = dateArray[0].trim() + dateArray[1].trim();
					columnIndex = ColumnIndexAnalysis.getColunmIndex(specificMonth, categoryIndex, allMonth, comCateAmount, mediaCateAmount);
				}
			}
			
			System.out.println(gid + " " + columnIndex);
		}		
	}
	
	public void testReduce(){
		Map<Integer, Integer> map = new TreeMap<Integer, Integer>();
		
		map.put(1, 3);
		map.put(50, 2);
		map.put(37, 4);
		map.put(90, 4);
		map.put(4, 4);
		map.put(127, 5);
		
		String gid = "aaaaa";
		StringBuffer output = new StringBuffer();
		output.append(gid + " ");
		for(Map.Entry<Integer, Integer> entry : map.entrySet()){
			String columnIndex = String.valueOf(entry.getKey());
			String amout = String.valueOf(entry.getValue());
			output.append(columnIndex + ":" + amout + " ");
		}
		
		System.out.println(output.toString().trim());
	}
	
}
