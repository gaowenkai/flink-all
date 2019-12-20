package com.jd.study.leetcode;

/**
 * 25. K 个一组翻转链表
 * https://blog.csdn.net/RAYFUXK/article/details/103373874
 */

public class reverseKGroup {
    public ListNode reverseKGroup(ListNode head, int k) {

        if (head == null || head.next == null) {
            return head;
        }

        ListNode dummy = new ListNode(0);
        ListNode pre = dummy;
        ListNode cur = head;
        pre.next = cur;
        ListNode start = null;
        ListNode end = null;
        ListNode after = null;

        while (cur!=null) {
            start = cur;
            for (int i = 1; i < k; i++) {
                if (cur == null) { break;}
                cur = cur.next;
            }
            if (cur == null) {break;}
            end = cur;
            after = cur.next;
            pre.next = reserve(start, end);
            start.next = after;
            pre = start;
            cur = after;
        }
        return dummy.next;
    }



    private ListNode reserve(ListNode s, ListNode e) {
        ListNode pre = null;
        ListNode current = s;
        ListNode after = null;
        while (pre != e && current != null) {
            after = current.next;
            current.next = pre;
            pre = current;
            current = after;
        }
        return e;
    }
}

