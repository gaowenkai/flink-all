package com.jd.study.leetcode;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

/**
 * DFS & 最短路径
 * BFS
 */
public class Soulation1036 {

    private static final int len = 5;

    private int[][] directions = new int[][] {
            {0, 1},
            {0, -1},
            {1, 0},
            {-1, 0}
    };


    public boolean isEscapePossible(int[][] blocked, int[] source, int[] target) {

        int[][] maze = new int[len][len];
        for(int[] b:blocked){
            maze[b[0]][b[1]] = -1;
        }
        return dfs(maze,source[0],source[1],target);

    }


    boolean isSafe(int[][] maze, int i ,int j){
        if(i>=0 && i<len && j>=0 && j<len && maze[i][j]!=-1){
            return true;
        }
        return false;
    }


    boolean dfs(int maze[][], int x, int y, int[] target){
        Stack<Integer[]> stack = new Stack<>();
        stack.push(new Integer[]{x,y});
        maze[x][y] = -1;
        while(!stack.isEmpty()){
            Integer[] pos = stack.pop();
            x = pos[0];
            y = pos[1];

            if (x==target[0] && y==target[1]){
                return true;
            }

            for(int[] d:directions){
                int i = x + d[0];
                int j = y + d[1];

                if (isSafe(maze,i,j)){
                    stack.push(new Integer[]{i,j});
                    maze[i][j] = -1;
                }
            }
        }
        return false;
    }


    void solveDFS(int[][] blocked, int[] source, int[] target){
        int[][] maze = new int[len][len];
        for(int[] b:blocked){
            maze[b[0]][b[1]] = -1;
        }
        for(int i=0;i<maze.length;i++){
            for(int j=0;j<maze[0].length;j++){
                if(maze[i][j]!=-1 && !(i==source[0] && j==source[1])){
                    maze[i][j] = Integer.MAX_VALUE;
                }
            }
        }

        dfsMin(maze,source[0],source[1],target);

        if(maze[target[0]][target[1]]<Integer.MAX_VALUE){
            System.out.println("dfs: " + maze[target[0]][target[1]]);
        }else{
            System.out.println("dfs no way");
        }


    }

    void dfsMin(int[][] maze,int x, int y, int[] target){
        if (x==target[0] && y==target[1]){
            return;
        }
        for(int[] d:directions) {
            int i = x + d[0];
            int j = y + d[1];

            if(isSafe(maze,i,j) && maze[i][j]>maze[x][y]+1){
                maze[i][j] = maze[x][y]+1;
                dfsMin(maze,i,j,target);
            }
        }
    }

    void bfsMin(int[][] blocked, int[] source, int[] target){
        int[][] maze = new int[len][len];
        for(int[] b:blocked){
            maze[b[0]][b[1]] = -1;
        }
        int x = source[0];
        int y = source[1];
        Queue<Integer[]> queue = new LinkedList<>();
        queue.add(new Integer[]{x,y});

        while(!queue.isEmpty()){
            Integer[] pos = queue.poll();
            x = pos[0];
            y = pos[1];
            for(int[] d:directions){
                int i = x + d[0];
                int j = y + d[1];
                if(isSafe(maze,i,j)){
                    maze[i][j] = maze[x][y]+1;
                    queue.add(new Integer[]{i,j});
                    if(i==target[0] && j==target[1]){
                        System.out.println("bfs: " + maze[i][j]);
                        return;
                    }
                }
            }

        }

    }

    public static void main(String[] args) {
        int[][] blocked = new int[][]{{0,1},{3,0},{1,1}}; //{{0,1},{2,0},{1,1}}
        int[] source = new int[]{0,0};
        int[] target = new int[]{0,2};
        Soulation1036 s = new Soulation1036();
        boolean res = s.isEscapePossible(blocked,source,target);
        System.out.println(res);
        if(res){
            s.solveDFS(blocked,source,target);
            s.bfsMin(blocked,source,target);
        }

    }
}
