package com.bonc.hbase.hdfs2hbase;

/**
 * 
 * @author xiabaike
 * @date 2016年5月31日
 */
public class Test {

	static long count;
	public static void hano(int n, char a, char b, char c) {
		if(n == 1) {
			System.out.printf("第%d次移动:\t圆盘从 %c 棒移动到%c 棒\n", ++count, a, c);
		}else{
			hano(n-1, a, c, b);
			System.out.printf("第%d次移动:\t圆盘从 %c 棒移动到%c 棒\n", ++count, a, c);
			hano(n-1, b, a, c);
		}
	}
	
	public static void main(String[] args) {
		hano(3, 'A', 'B', 'C');
	}
	
}

class Dec{
	static int num = 0;
	
	private String a;

	public String getA() {
		return a;
	}

	public void setA(String a) {
		this.a = a;
	}
	
}