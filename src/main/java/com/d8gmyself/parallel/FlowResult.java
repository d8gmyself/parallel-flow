package com.d8gmyself.parallel;

import java.util.Collections;
import java.util.Map;

/**
 * DAG执行结果，不可变对象，持有目标结果、所有节点状态快照和预计算的Mermaid图
 *
 * @param <O> 目标节点返回值类型
 */
public class FlowResult<O> {

    private final boolean success;
    private final O resultValue;
    private final Throwable exception;
    private final Map<String, NodeState> nodeStates;
    private final String mermaid;
    private final long durationMs;

    FlowResult(boolean success, O resultValue, Throwable exception,
               Map<String, NodeState> nodeStates, String mermaid, long durationMs) {
        this.success = success;
        this.resultValue = resultValue;
        this.exception = exception;
        this.nodeStates = Collections.unmodifiableMap(nodeStates);
        this.mermaid = mermaid;
        this.durationMs = durationMs;
    }

    /**
     * 获取目标节点结果，失败时抛出 ParallelFlowException
     */
    public O get() {
        if (!success) {
            if (exception instanceof ParallelFlowException) {
                throw (ParallelFlowException) exception;
            }
            throw new ParallelFlowException("Execution failed", exception);
        }
        return resultValue;
    }

    public O orElse(O defaultValue) {
        return success ? resultValue : defaultValue;
    }

    public boolean isSuccess() {
        return success;
    }

    public NodeState getNodeState(String name) {
        return nodeStates.get(name);
    }

    public Map<String, NodeState> allNodeStates() {
        return nodeStates;
    }

    /**
     * 返回预计算的Mermaid DAG状态图
     * <p>弱依赖节点虚线边框，失败节点红色边框，两者叠加</p>
     */
    public String getMermaid() {
        return mermaid;
    }

    public long getDurationMs() {
        return durationMs;
    }
}
