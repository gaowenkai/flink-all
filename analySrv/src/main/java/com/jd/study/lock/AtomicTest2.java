package com.jd.study.lock;

import java.util.concurrent.atomic.AtomicStampedReference;


public class AtomicTest2 {

    //  设置账户初始值小于20，这是一个需要被充值的客户
    static AtomicStampedReference<Integer> money = new AtomicStampedReference<>(19,0);

    public static void main(String[] args) {
        // 模拟多个线程同时更新后台数据库，为用户充值
        for (int i=0;i<3;i++){
            new Thread(){
                final int timestamp = money.getStamp();
                public void run(){
                    while(true){
                        while (true){
                            Integer m =money.getReference();
                            if (m<20){
                                if (money.compareAndSet(m,m+20,timestamp,timestamp+1)){
                                    System.out.println("余额小于20，充值成功，余额："+money.getReference()+"元"+" timestamp:"+money.getStamp());
                                    break;
                                }
                            }else{
                                //  余额大于20，无需充值
                                break;
                            }
                        }
                    }
                }
            }.start();
        }

        //  用户消费线程，模拟消费行为
        new Thread(){
            public void run(){
                for (int i=0;i<10;i++){
                    while (true){
                        int timestamp = money.getStamp();
                        Integer m =money.getReference();
                        if (m>10){
                            System.out.println("大于10元");
                            if (money.compareAndSet(m,m-10,timestamp,timestamp+1)){
                                System.out.println("消费10元，余额："+money.getReference()+"元"+" timestamp:"+money.getStamp());
                                break;
                            }
                        }else {
                            System.out.println("没有足够的金额");
                            break;
                        }
                    }
                    try{
                        Thread.sleep(10);
                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }
            }
        }.start();
    }
}