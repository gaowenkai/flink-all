package com.jd.study.leetcode;

public class QuickSort {
    public static void quickSort(int[] arr,int low,int high){
        int i,j,temp,t;
        if(low>high){
            return;
        }
        temp = arr[low];
        i = low;
        j = high;
        while(i<j){
            while(temp<=arr[j]&&i<j){
                j--;
            }
            while(temp>=arr[i]&&i<j){
                i++;
            }
            if(i<j){
                t = arr[i];
                arr[i] = arr[j];
                arr[j] = t;
            }
        }
        arr[low] = arr[i];
        arr[i] = temp;

        quickSort(arr,low,j-1);
        quickSort(arr,j+1,high);
    }

    public static void main(String[] args) {
        int[] arr = {10,4,2,6,3,2,7,2,9};
        quickSort(arr,0,arr.length-1);
        for (int i=0;i<arr.length-1;i++){
            System.out.println(arr[i]);
        }
    }
}
