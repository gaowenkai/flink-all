package com.jd.kafka;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

//窗口定时触发器（每10条数据触发一次）
public class CustomProcessingTimeTrigger extends Trigger<Object, TimeWindow> {

        private static final long serialVersionUID = -4372617074659249920L;
        private static int flag = 0;

    @Override
    public TriggerResult onElement(Object o, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        triggerContext.registerProcessingTimeTimer(timeWindow.maxTimestamp());
        if (flag >= 9){
            flag = 0;
            return TriggerResult.FIRE;
        }else{
            flag++;
        }
        System.out.println(flag);
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        triggerContext.deleteProcessingTimeTimer(timeWindow.maxTimestamp());

    }
}
