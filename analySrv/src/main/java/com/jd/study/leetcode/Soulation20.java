package com.jd.study.leetcode;

import java.util.Stack;

/**
 * 20. 有效的括号
 */
public class Soulation20 {
    public boolean isValid(String s) {
        if (s.length() % 2 != 0) {
            return false;
        }
        Stack<Character> stack = new Stack();
        int index = 0;
        for (int i = 0; i < s.length(); i++) {
            switch (s.charAt(i)) {
                case '(':
                case '{':
                case '[':
                    stack.push(s.charAt(i));
                    index ++;
                    continue;
                case ')':
                    if(--index <0){
                        return false;
                    }else {
                        if (stack.pop() != '(') {
                            return false;
                        } else {
                            continue;
                        }
                    }
                case '}':
                    if(--index <0){
                        return false;
                    }else {
                        if (stack.pop() != '{') {
                            return false;
                        } else {
                            continue;
                        }
                    }
                case ']':
                    if(--index <0){
                        return false;
                    }else {
                        if (stack.pop() != '[') {
                            return false;
                        } else {
                            continue;
                        }
                    }
                default:
                    return false;
            }
        }
        return stack.isEmpty();
    }

    public static void main(String[] args) {
        boolean res = new Soulation20().isValid("{})(");
        System.out.println(res);
    }
}
