package com.jd.study.lock;

import java.util.concurrent.atomic.AtomicReference;


public class AtomicTest {
    static AtomicReference<Integer> money = new AtomicReference<>();
    public static void main(String[] args) {
        //  设置账户初始值小于20，这是一个需要被充值的客户
        money.set(19);

        // 模拟多个线程同时更新后台数据库，为用户充值
        for (int i=0;i<3;i++){
            new Thread(){
                public void run(){
                    while(true){
                        while (true){
                            Integer m =money.get();
                            if (m<20){
                                if (money.compareAndSet(m,m+20)){
                                    System.out.println("余额小于20，充值成功，余额："+money.get()+"元");
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
                for (int i=0;i<100;i++){
                    while (true){
                        Integer m =money.get();
                        if (m>10){
                            System.out.println("大于10元");
                            if (money.compareAndSet(m,m-10)){
                                System.out.println("消费10元，余额："+money.get()+"元");
                                break;
                            }
                        }else {
                            System.out.println("没有足够的金额");
                            break;
                        }
                    }
                    try{
                        Thread.sleep(100);
                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }
            }
        }.start();
    }
}