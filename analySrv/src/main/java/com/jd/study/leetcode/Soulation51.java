package com.jd.study.leetcode;

import java.util.ArrayList;
import java.util.List;


/**
 * n皇后
 */
public class Soulation51 {

    public List<List<String>> solveNQueens(int n) {
        List<List<String>> lists = new ArrayList<>();
        int[] cols = new int[n];
        backtracting(n,0,cols,lists);
        return lists;

    }

    boolean check(int row, int col, int[] cols){
        for(int r=0;r<row;r++){
            if(cols[r]==col||(row-r)==Math.abs(cols[r]-col)){
                return false;
            }
        }
        return true;
    }

    void backtracting(int n, int row, int[] cols,List<List<String>> lists){
        if (row==n){
            char[][] s = new char[n][n];
            for(int i = 0; i < n; i++){
                for(int j = 0; j < n; j++){
                    s[i][j] = '.';
                }
            }
            for(int i=0;i<n;i++){
                s[i][cols[i]] = 'Q';
            }
            List<String> list = new ArrayList<>();
            for(int i = 0; i < n; i++){
                list.add(new String(s[i]));
            }
            lists.add(list);
            return;
        }

        for(int col=0;col<n;col++){
            cols[row] = col;
            if(check(row,col,cols)){
                backtracting(n,row+1,cols,lists);
            }
            cols[row] = -1;
        }
    }

    public static void main(String[] args) {
        List<List<String>> res = new Soulation51().solveNQueens(4);
        System.out.println(res);
    }


}
