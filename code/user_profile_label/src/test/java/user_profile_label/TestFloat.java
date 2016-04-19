package user_profile_label;

public class TestFloat {
	public static void main(String[] args) {
//		int max_level = 10000000 ;
//		float init_level = 1.0F/max_level;
//		System.out.println(init_level);
//		
//		String a = "97=0.097000";
//		int aa = Integer.parseInt(a.split("=")[0]);
//		float bb = Float.parseFloat(a.split("=")[1]);
//		System.out.println();
//		
//		float a = 1.0F/100;
//		float bb = 0.01F;
//		float aa = (1-a)*bb;
//		System.out.println(aa);
		
		for (int i = 1; i <= 100; i++) {
			float prod = (float)i/(float)1000;
			String a = String.format("%f", prod);
			System.out.println(a);
		}
		
		System.out.println(String.format("%d", 100));
	}
}
