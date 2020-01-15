package com.jd.study.leetcode;

import java.util.ArrayList;
import java.util.List;

public class Soulation39 {

    public List<List<Integer>> combinationSum(int[] candidates, int target) {
        List<List<Integer>> lists = new ArrayList<>(); // lists[list...]
        List<Integer> list = new ArrayList<>();
        if (candidates==null||candidates.length==0||target<0){return lists;}
        getlist(0,candidates,target,list,lists);
        return lists;
    }

    public static void getlist(int start ,int[] candidates, int target, List<Integer> list,List<List<Integer>> lists){
        if(target<0) return;

        if(target==0){
            lists.add(new ArrayList<>(list));
        }

        if(target>0) {
            for (int i = start; i < candidates.length; i++) {
                list.add(candidates[i]);
                getlist(i, candidates, target - candidates[i], list,lists);
                list.remove(list.size() - 1);
            }
        }
    }
}
