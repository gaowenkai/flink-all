package com.jd.study.leetcode;

import java.util.ArrayList;
import java.util.List;

public class Soulation212 {
    class TreeNode{
        char val;
        TreeNode[] nextNodeList;
        boolean isEnd;//判断是否到达单词末尾
        TreeNode(){
            val = ' ';
            nextNodeList = new TreeNode[26];
            isEnd = false;
        }
    }

    TreeNode root;

    public void insert(String word){
        TreeNode temp = root;
        for(int i=0;i<word.length();i++){
            char ch = word.charAt(i);
            int index = ch-'a';
            TreeNode node = temp.nextNodeList[index];
            if(node==null){//该节点前面未插入该字母值
                node = new TreeNode();
                node.val = ch;
                temp.nextNodeList[index] = node;
            }
            temp = node;
        }
        temp.isEnd = true;
    }

    public List<String> findWords(char[][] board, String[] words) {
        root = new TreeNode();
        List<String> list = new ArrayList<String>();
        int row = board.length;
        if (row < 1) return list;
        int col = board[0].length;
        if (col < 1) return list;
        boolean[][] isVisit = new boolean[row][col];
        for (int i=0;i<words.length;i++){
            insert(words[i]);
        }
        for(int i=0;i<row;i++){
            for(int j=0;j<col;j++){
                char ch = board[i][j];
                int index = ch - 'a';
                TreeNode node = root.nextNodeList[index];
                if (node!=null){
                    isVisit[i][j] = true;
                    searchword(ch+"",board,i,j,node,list,isVisit);
                    isVisit[i][j] = false;
                }
            }
        }
        return list;
    }

    public void searchword(String s,
                            char[][] board,
                            int i, int j,
                            TreeNode root,
                            List<String> list,
                            boolean[][] isVisit) {
        if(root==null) return;
        if(root.isEnd==true){
            if(!list.contains(s)){
                list.add(s);
            }
        }
        if(i-1>=0&&isVisit[i-1][j]==false){
            isVisit[i-1][j] = true;
            char ch = board[i-1][j];
            int index = ch - 'a';
            TreeNode node = root.nextNodeList[index];
            searchword(s+ch,board,i-1,j,node,list,isVisit);
            isVisit[i-1][j] = false;
        }
        if(i+1<board.length&&isVisit[i+1][j]==false){
            isVisit[i+1][j] = true;
            char ch = board[i+1][j];
            int index = ch - 'a';
            TreeNode node = root.nextNodeList[index];
            searchword(s+ch,board,i+1,j,node,list,isVisit);
            isVisit[i+1][j] = false;
        }
        if(j+1<board[0].length&&isVisit[i][j+1]==false){
            isVisit[i][j+1] = true;
            char ch = board[i][j+1];
            int index = ch - 'a';
            TreeNode node = root.nextNodeList[index];
            searchword(s+ch,board,i,j+1,node,list,isVisit);
            isVisit[i][j+1] = false;
        }
        if(j-1>=0&&isVisit[i][j-1]==false){
            isVisit[i][j-1] = true;
            char ch = board[i][j-1];
            int index = ch - 'a';
            TreeNode node = root.nextNodeList[index];
            searchword(s+ch,board,i,j-1,node,list,isVisit);
            isVisit[i][j-1] = false;
        }

    }

    public static void main(String[] args) {
        System.out.println('b'-'a');
    }

}
