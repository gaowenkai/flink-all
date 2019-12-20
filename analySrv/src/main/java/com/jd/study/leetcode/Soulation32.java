package com.jd.study.leetcode;

import java.util.Stack;

/**
 * 32. 最长有效括号
 */

public class Soulation32 {
    public int longestValidParentheses(String s) {
        if (s.length()==0){
            return 0;
        }
        Stack<Integer> stack = new Stack();
        stack.push(-1);
        int max = 0;

        for(int i=0;i<s.length();i++){
            if (s.charAt(i)=='('){
                stack.push(i);
            }
            else{
                stack.pop();
                if (stack.size()!=0) {
                    max = Math.max(max, i-stack.peek());
                }else{
                    stack.push(i);
                }
            }
        }

        return max;
    }

    public static void main(String[] args) {
        int res = new Soulation32().longestValidParentheses("()(");
        // )(()))(()
        System.out.println(res);
    }


}
