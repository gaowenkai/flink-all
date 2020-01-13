package com.jd.study.leetcode;

public class Soulation147 {
    public ListNode insertionSortList(ListNode head) {
        ListNode dummy = new ListNode(0);
        dummy.next = head;
        ListNode pre = dummy;
        ListNode cur = head;
        ListNode next;

        while(cur!=null){
            next = cur.next;
            if(next!=null&&cur.val>next.val){
                while(pre.next!=null&&pre.next.val<next.val){
                    pre = pre.next;
                }
                ListNode  t = pre.next;
                pre.next = next;
                cur.next = next.next;
                next.next = t;

                pre = dummy;//复位
            }else{
                cur =next;
            }
        }



        return dummy.next;
    }
}
