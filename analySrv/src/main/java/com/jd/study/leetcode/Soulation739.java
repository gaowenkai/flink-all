package com.jd.study.leetcode;

import java.util.Stack;


/**
 * 739. 每日温度
 */
public class Soulation739 {
    public int[] dailyTemperatures(int[] T) {
        int[] res = new int[T.length];
        Stack<Integer> s = new Stack<>();
        int pre=0;
        int cur=0;
        for(int i=0;i<T.length;i++){

            if(s.size()==0){
                s.push(i);
                continue;
            }
            cur = T[i];
            pre = T[s.peek()];
            while(cur>pre && s.size()!=0){
                int index = s.pop();
                res[index] = i-index;
                if(s.size()==0){break;}
                pre = T[s.peek()];
            }
            s.push(i);

        }
        if (s.size()!=0){
            while(s.size()!=0){
                int index = s.pop();
                res[index] = 0;
            }
        }


        return res;
    }

    public static void main(String[] args) {
        int[] res = new Soulation739().dailyTemperatures(new int[]{25, 21, 19, 22, 26, 23});
        for (int i = 0; i < res.length; i++) {
            System.out.print(res[i] + ",");
        }
    }
}