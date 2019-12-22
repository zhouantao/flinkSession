package com.test.session;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger.OnMergeContext;
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

@PublicEvolving
public class MyTrigger<W extends Window> extends Trigger<Object, W> {
    private static final long serialVersionUID = 1L;
    private final long interval;
    private final ReducingStateDescriptor<Long> stateDesc;
    private Boolean isHasNewData = false;

    public Boolean getHasNewData() {
        return isHasNewData;
    }

    public void setHasNewData(Boolean hasNewData) {
        isHasNewData = hasNewData;
    }

    private MyTrigger(long interval) {
        this.stateDesc = new ReducingStateDescriptor("fire-time", new MyTrigger.Min(), LongSerializer.INSTANCE);
        this.interval = interval;
    }

    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        setHasNewData(true);
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            ReducingState<Long> fireTimestamp = (ReducingState)ctx.getPartitionedState(this.stateDesc);
            if (fireTimestamp.get() == null) {
                long start = timestamp - timestamp % this.interval;
                long nextFireTimestamp = start + this.interval;
                ctx.registerEventTimeTimer(nextFireTimestamp);
                fireTimestamp.add(nextFireTimestamp);
            }

            return TriggerResult.CONTINUE;
        }
    }

    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        if (time == window.maxTimestamp() && getHasNewData()) {
            System.out.println("！！！！！！！到达窗口最大时间！！！");
            return TriggerResult.FIRE;
        } else {
            ReducingState<Long> fireTimestampState = (ReducingState)ctx.getPartitionedState(this.stateDesc);
            Long fireTimestamp = (Long)fireTimestampState.get();
            if (fireTimestamp != null && fireTimestamp == time) {
                fireTimestampState.clear();
                fireTimestampState.add(time + this.interval);
                ctx.registerEventTimeTimer(time + this.interval);
                clearTimerForState(ctx);
                setHasNewData(false);
                return TriggerResult.FIRE;
            } else {
                return TriggerResult.CONTINUE;
            }
        }
    }

    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    private void clearTimerForState(TriggerContext ctx) throws Exception {
        Long timestamp = ctx.getPartitionedState(stateDesc).get();
        if (timestamp != null) {
            ctx.deleteEventTimeTimer(timestamp);
        }
    }

    public void clear(W window, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = (ReducingState)ctx.getPartitionedState(this.stateDesc);
        Long timestamp = (Long)fireTimestamp.get();
        if (timestamp != null) {
            ctx.deleteEventTimeTimer(timestamp);
            fireTimestamp.clear();
        }

    }

    public boolean canMerge() {
        return true;
    }

    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        ctx.mergePartitionedState(this.stateDesc);
        Long nextFireTimestamp = (Long)((ReducingState)ctx.getPartitionedState(this.stateDesc)).get();
        if (nextFireTimestamp != null) {
            ctx.registerEventTimeTimer(nextFireTimestamp);
        }

    }

    public String toString() {
        return "ContinuousEventTimeTrigger(" + this.interval + ")";
    }

    @VisibleForTesting
    public long getInterval() {
        return this.interval;
    }

    public static <W extends Window> MyTrigger<W> of(Time interval) {
        return new MyTrigger(interval.toMilliseconds());
    }

    private static class Min implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        private Min() {
        }

        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }
}
