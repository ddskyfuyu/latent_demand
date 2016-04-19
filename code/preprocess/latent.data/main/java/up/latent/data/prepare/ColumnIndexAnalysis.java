package up.latent.data.prepare;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ColumnIndexAnalysis {

	
	public static void main(String[] args) {
		String allMonth = "201511,201512";
		String specificMonth = "201512";
		String categoryIndex = "25";
		int comCateAmount = 977;
		int mediaCateAmount = 947;
		int colunm = ColumnIndexAnalysis.getColunmIndex(specificMonth, categoryIndex, allMonth, comCateAmount, mediaCateAmount);
		System.out.println(colunm);

	}
	
	/**
	 * * 根据选定的月份列表，和指定的月份与类别索引号，得到输出结果的中列号
	 * @param specificMonth	指定的月份
	 * @param categoryIndex	品类索引号
	 * @param allMonth	全部月分列表，格式为 “201509,201510,101511”
	 * @param comCateAmount	电商品类总数量
	 * @param mediaCateAmount	媒体类目总数量
	 * @return	该月份、品类索引号的列号
	 */
	public static int getColunmIndex(String specificMonth, String categoryIndex, String allMonth, int comCateAmount, int mediaCateAmount){
		int colunmIndex = -1;
		List<String> monthList = Arrays.asList(allMonth.trim().split(","));
		int totalAmount = comCateAmount + mediaCateAmount;
		int monthIndex = -1;
		monthIndex = monthList.indexOf(specificMonth.trim());
		if(monthIndex != -1){
			colunmIndex = monthIndex * totalAmount + Integer.valueOf(categoryIndex);
		}
		return colunmIndex;
	}

}
