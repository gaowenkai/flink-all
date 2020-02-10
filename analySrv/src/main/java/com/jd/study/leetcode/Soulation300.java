package com.jd.study.leetcode;

import java.util.HashMap;

/**
 * 300. 最长上升子序列
 */

public class Soulation300 {

    static int max;
    static HashMap<Integer,Integer> cache;

    public int lengthOfLIS(int[] nums) {
        //init
        max = 0;
        cache = new HashMap<>();
        if(nums.length==0) {
            return 0;
        }else{
            max = 1;
            f(nums,nums.length);
            return max;
        }

    }

    public int f(int[] nums, int n){
        if (cache.containsKey(n)){
            return cache.get(n);
        }
        if (n<=1){
            return n;
        }

        int result;
        int end = 1;

        for(int i=1;i<n;i++){
            result = f(nums,i);
            if(nums[i-1]<nums[n-1] && result+1>end){
                end = result+1;
            }
        }
        if(max<end){
            max = end;
        }

        cache.put(n,end);
        return end;

    }

    public static void main(String[] args) {
        int[] nums = new int[]{-2,-1};
        int res = new Soulation300().lengthOfLIS(nums);
        System.out.println(res);
    }
}
