package com.jd.study.leetcode;

public class Soulation91 {
    public int numDecodings(String s) {
        if (s.charAt(0)=='0') return 0;
        char[] ss = s.toCharArray();
        return decode(ss,ss.length-1);
    }

    public static int decode(char[] s, int i){
        int count = 0;
        if(i<=0){
            return 1;
        }
        char cur = s[i];
        char pre = s[i-1];
        if(cur>'0'){
            count = decode(s,i-1);
        }
        if(pre=='1'||(pre=='2'&&cur<='6')){
            count += decode(s,i-2);
        }

        return count;
    }
}
