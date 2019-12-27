package com.jd.study.leetcode;

import java.util.*;


/**
 * 347. 前 K 个高频元素
 */

public class Soulation347 {
    public List<Integer> topKFrequent(int[] nums, int k) {
        HashMap<Integer, Integer> map = new HashMap<>();
        for (int num: nums){
            map.put(num,map.getOrDefault(num,0)+1);
        }

        PriorityQueue<Integer> pq = new PriorityQueue<>(new Comparator<Integer>(){
            public int compare(Integer a, Integer b){
                return map.get(b)-map.get(a);
            }
        });
        for (Map.Entry<Integer,Integer> e :map.entrySet()){
            pq.offer(e.getKey());
        }
        List<Integer> res = new LinkedList<>();
        while(k>0){
            k -= 1;
            res.add(pq.poll());
        }

        return res;

    }
}
