package com.jd.study.leetcode;


/**
 * 70. 爬楼梯
 */
public class Soulation70 {
    public int climbStairs(int n) {
        if(n==0) return 0;
        if(n==1) return 1;
        if(n==2) return 2;

        return climbStairs(n-1) + climbStairs(n-2);
    }

    public int climbStairsDP(int n){
        if(n==0) return 0;
        if(n==1) return 1;
        int[] dp = new int[n];
        dp[0] = 1;
        dp[1] = 2;
        for(int i=2;i<n;i++){
            dp[i] = dp[i-1]+dp[i-2];
        }

        return dp[n-1];
    }
}
