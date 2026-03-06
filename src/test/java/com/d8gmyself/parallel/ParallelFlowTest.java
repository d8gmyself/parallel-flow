package com.d8gmyself.parallel;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class ParallelFlowTest {

    // ========== 1. Simple fan-out / fan-in (product detail page aggregation) ==========

    @Test
    public void testSimpleFanOutFanIn() {
        TaskNode<String> productInfo = TaskNode.of("productInfo", ctx -> {
            String id = ctx.<String>get("input");
            Thread.sleep(50);
            return "Product-" + id;
        });

        TaskNode<String> price = TaskNode.of("price", ctx -> {
            Thread.sleep(30);
            return "Price-99";
        });

        TaskNode<String> reviews = TaskNode.of("reviews", ctx -> {
            Thread.sleep(40);
            return "Reviews-5star";
        });

        TaskNode<String> aggregate = TaskNode.<String>builder("aggregate", ctx -> {
            String p = productInfo.get();
            String pr = price.get();
            String r = reviews.get();
            return p + "|" + pr + "|" + r;
        }).dependsOn(productInfo, price, reviews).build();

        FlowContext flowCtx = new FlowContext();
        flowCtx.put("input", "SKU-001");
        String result = ParallelFlow.start(aggregate, flowCtx);
        assertEquals("Product-SKU-001|Price-99|Reviews-5star", result);
    }

    // ========== 2. Multi-layer DAG dependency ==========

    @Test
    public void testMultiLayerDag() {
        TaskNode<String> nodeA = TaskNode.of("A", ctx -> {
            String input = ctx.<String>get("input");
            return "A(" + input + ")";
        });

        TaskNode<String> nodeB = TaskNode.<String>builder("B", ctx -> {
            String a = nodeA.get();
            return "B(" + a + ")";
        }).dependsOn(nodeA).build();

        TaskNode<String> nodeC = TaskNode.<String>builder("C", ctx -> {
            String a = nodeA.get();
            return "C(" + a + ")";
        }).dependsOn(nodeA).build();

        TaskNode<String> nodeD = TaskNode.<String>builder("D", ctx -> {
            String b = nodeB.get();
            String c = nodeC.get();
            return "D(" + b + "," + c + ")";
        }).dependsOn(nodeB, nodeC).build();

        FlowContext flowCtx = new FlowContext();
        flowCtx.put("input", "start");
        String result = ParallelFlow.start(nodeD, flowCtx);
        assertEquals("D(B(A(start)),C(A(start)))", result);
    }

    // ========== 3. Weak dependency failure + fallback ==========

    @Test
    public void testOptionalDependencyWithFallback() {
        TaskNode<String> mainService = TaskNode.of("main", ctx -> "MainData");

        TaskNode<String> optionalService = TaskNode.<String>builder("optional", ctx -> {
            throw new RuntimeException("Service down");
        }).fallback(ex -> "FallbackData").build();

        TaskNode<String> aggregate = TaskNode.<String>builder("aggregate", ctx -> {
            String main = mainService.get();
            String opt = optionalService.orElse("DefaultData");
            return main + "|" + opt;
        }).dependsOn(mainService).weakDependsOn(optionalService).build();

        String result = ParallelFlow.start(aggregate);
        assertEquals("MainData|FallbackData", result);
    }

    // ========== 4. Strong dependency failure → overall failure ==========

    @Test
    public void testRequiredDependencyFailure() {
        TaskNode<String> failingService = TaskNode.of("failing", ctx -> {
            throw new RuntimeException("Critical failure");
        });

        TaskNode<String> downstream = TaskNode.<String>builder("downstream", ctx -> {
            return failingService.get();
        }).dependsOn(failingService).build();

        try {
            ParallelFlow.start(downstream);
            fail("Expected ParallelFlowException");
        } catch (ParallelFlowException e) {
            assertTrue(e.getMessage().contains("failed"));
        }
    }

    // ========== 5. Timeout trigger ==========

    @Test
    public void testTimeout() {
        TaskNode<String> slowTask = TaskNode.<String>builder("slow", ctx -> {
            Thread.sleep(2000);
            return "done";
        }).timeout(100).build();

        try {
            ParallelFlow.start(slowTask);
            fail("Expected ParallelFlowException due to timeout");
        } catch (ParallelFlowException e) {
            assertTrue(e.getMessage().contains("timed out") || e.getMessage().contains("failed"));
        }
    }

    // ========== 6. Retry success ==========

    @Test
    public void testRetrySuccess() {
        AtomicInteger attempts = new AtomicInteger(0);

        TaskNode<String> retryTask = TaskNode.<String>builder("retryTask", ctx -> {
            if (attempts.incrementAndGet() < 3) {
                throw new RuntimeException("Transient error, attempt=" + attempts.get());
            }
            return "Success on attempt " + attempts.get();
        }).retry(3).build();

        String result = ParallelFlow.start(retryTask);
        assertEquals("Success on attempt 3", result);
        assertEquals(3, attempts.get());
    }

    // ========== 7. Nested fan-out / fan-in ==========

    @Test
    public void testNestedFanOutFanIn() {
        TaskNode<String> a1 = TaskNode.of("A1", ctx -> {
            String input = ctx.<String>get("input");
            return "A1-" + input;
        });
        TaskNode<String> a2 = TaskNode.of("A2", ctx -> {
            String input = ctx.<String>get("input");
            return "A2-" + input;
        });

        TaskNode<String> b = TaskNode.<String>builder("B", ctx -> {
            return "B(" + a1.get() + "," + a2.get() + ")";
        }).dependsOn(a1, a2).build();

        TaskNode<String> c = TaskNode.<String>builder("C", ctx -> {
            return "C(" + a1.get() + ")";
        }).dependsOn(a1).build();

        TaskNode<String> d = TaskNode.<String>builder("D", ctx -> {
            return "D(" + b.get() + "," + c.get() + ")";
        }).dependsOn(b, c).build();

        FlowContext flowCtx = new FlowContext();
        flowCtx.put("input", "X");
        String result = ParallelFlow.start(d, flowCtx);
        assertEquals("D(B(A1-X,A2-X),C(A1-X))", result);
    }

    // ========== 8. Circular dependency — structurally prevented by Builder ==========
    // Builder模式下，节点A必须先于节点B存在才能被B依赖，因此循环依赖在编译期就不可能构造
    // ParallelFlow中的拓扑排序仍保留循环检测作为防御性校验

    // ========== 9. Builder with custom executor and listener ==========

    @Test
    public void testBuilderWithListener() {
        ExecutorService customExecutor = Executors.newFixedThreadPool(4);
        List<String> events = Collections.synchronizedList(new ArrayList<String>());

        TaskLifecycleListener listener = new TaskLifecycleListener() {
            @Override
            public void onStart(TaskEvent event) {
                events.add("start:" + event.getTaskName());
            }

            @Override
            public void onSuccess(TaskEvent event) {
                events.add("success:" + event.getTaskName());
            }
        };

        TaskNode<String> task1 = TaskNode.of("task1", ctx -> {
            Thread.sleep(20);
            return "Result1";
        });

        TaskNode<String> task2 = TaskNode.of("task2", ctx -> {
            Thread.sleep(20);
            return "Result2";
        });

        TaskNode<String> merge = TaskNode.<String>builder("merge", ctx ->
                task1.get() + "+" + task2.get()
        ).dependsOn(task1, task2).build();
        String result = ParallelFlow.builder().listener(listener).executor(customExecutor).build().run(merge);
        assertEquals("Result1+Result2", result);

        assertTrue(events.contains("start:task1"));
        assertTrue(events.contains("success:task1"));
        assertTrue(events.contains("start:merge"));
        assertTrue(events.contains("success:merge"));

        customExecutor.shutdown();
    }

    // ========== 10. Circuit breaker test ==========

    @Test
    public void testCircuitBreaker() {
        DefaultCircuitBreaker cb = new DefaultCircuitBreaker(3, 5000);
        AtomicInteger callCount = new AtomicInteger(0);

        cb.recordFailure("failing");
        cb.recordFailure("failing");
        cb.recordFailure("failing");

        assertFalse("Circuit should be open", cb.allowRequest("failing"));

        TaskNode<String> task = TaskNode.<String>builder("failing", ctx -> {
            callCount.incrementAndGet();
            return "should not reach";
        }).circuitBreaker(cb).build();

        try {
            ParallelFlow.start(task);
            fail("Expected ParallelFlowException due to open circuit");
        } catch (ParallelFlowException e) {
            assertTrue(e.getMessage().contains("Circuit breaker") || e.getMessage().contains("failed"));
        }

        assertEquals(0, callCount.get());
    }

    // ========== 11. Duplicate task name detection ==========

    @Test
    public void testDuplicateTaskNameDetection() {
        TaskNode<String> a1 = TaskNode.of("A", ctx -> "first");
        TaskNode<String> a2 = TaskNode.of("A", ctx -> "second");

        TaskNode<String> merge = TaskNode.<String>builder("merge", ctx ->
                a1.get() + "+" + a2.get()
        ).dependsOn(a1, a2).build();

        try {
            ParallelFlow.start(merge);
            fail("Expected ParallelFlowException for duplicate task name");
        } catch (ParallelFlowException e) {
            assertTrue(e.getMessage().contains("Duplicate task name"));
        }
    }

    // ========== 12. Timeout wins race — node state must not be polluted ==========

    @Test
    public void testTimeoutWinsRaceNoStatePollution() {
        TaskNode<String> slow = TaskNode.<String>builder("slow", ctx -> {
            Thread.sleep(500);
            return "late-result";
        }).timeout(50).build();

        TaskNode<String> downstream = TaskNode.<String>builder("downstream", ctx -> {
            return slow.orElse("not-available");
        }).dependsOn(slow).build();

        try {
            ParallelFlow.start(downstream);
            fail("Expected ParallelFlowException due to timeout");
        } catch (ParallelFlowException e) {
            assertTrue(e.getMessage().contains("timed out") || e.getMessage().contains("failed"));
        }
    }

    // ========== 13. Timeout default with downstream ==========

    @Test
    public void testTimeoutDefaultWithDownstream() {
        TaskNode<String> slow = TaskNode.<String>builder("slow", ctx -> {
            Thread.sleep(2000);
            return "slow-result";
        }).timeout(50).timeoutDefault("timeout-default").build();

        TaskNode<String> downstream = TaskNode.<String>builder("downstream", ctx -> {
            return "got:" + slow.get();
        }).dependsOn(slow).build();

        String result = ParallelFlow.start(downstream);
        assertEquals("got:timeout-default", result);
    }

    // ========== 14. Timeout default (standalone) ==========

    @Test
    public void testTimeoutDefault() {
        TaskNode<String> slow = TaskNode.<String>builder("slow", ctx -> {
            Thread.sleep(2000);
            return "slow-result";
        }).timeout(50).timeoutDefault("default-value").build();

        String result = ParallelFlow.start(slow);
        assertEquals("default-value", result);
    }

    // ========== 15. Retry exhaustion with fallback ==========

    @Test
    public void testRetryExhaustionWithFallback() {
        AtomicInteger attempts = new AtomicInteger(0);

        TaskNode<String> task = TaskNode.<String>builder("retryFallback", ctx -> {
            attempts.incrementAndGet();
            throw new RuntimeException("Always fails");
        }).retry(2).fallback(ex -> "FallbackValue").build();

        String result = ParallelFlow.start(task);
        assertEquals("FallbackValue", result);
        assertEquals(3, attempts.get());
    }

    // ========== 16. Default timeout from ParallelFlow ==========

    @Test
    public void testDefaultTimeout() {
        ParallelFlow flow = ParallelFlow.builder()
                .defaultTaskTimeoutMs(50)
                .build();

        TaskNode<String> slow = TaskNode.<String>of("slow", ctx -> {
            Thread.sleep(2000);
            return "done";
        });

        try {
            flow.run(slow);
            fail("Expected ParallelFlowException due to default timeout");
        } catch (ParallelFlowException e) {
            assertTrue(e.getMessage().contains("timed out after 50ms"));
        }

        TaskNode<String> overridden = TaskNode.<String>builder("overridden", ctx -> {
            Thread.sleep(2000);
            return "done";
        }).timeout(30).build();

        try {
            flow.run(overridden);
            fail("Expected ParallelFlowException due to node timeout");
        } catch (ParallelFlowException e) {
            assertTrue(e.getMessage().contains("timed out after 30ms"));
        }
    }

    // ========== 17. Default timeout with timeoutDefault ==========

    @Test
    public void testDefaultTimeoutWithFallback() {
        ParallelFlow flow = ParallelFlow.builder()
                .defaultTaskTimeoutMs(50)
                .build();

        TaskNode<String> slow = TaskNode.<String>builder("slow", ctx -> {
            Thread.sleep(2000);
            return "slow-result";
        }).timeoutDefault("default-value").build();

        String result = flow.run(slow);
        assertEquals("default-value", result);
    }

    // ========== 18. ExecutionResult — success scenario ==========

    @Test
    public void testExecuteForResultSuccess() {
        TaskNode<String> main = TaskNode.of("main", ctx -> "MainData");

        TaskNode<String> opt = TaskNode.<String>builder("opt", ctx -> "OptData")
                .build();

        TaskNode<String> aggregate = TaskNode.<String>builder("aggregate", ctx ->
                main.get() + "|" + opt.get()
        ).dependsOn(main).weakDependsOn(opt).build();

        FlowResult<String> er = ParallelFlow.tryStart(aggregate);

        // 结果
        assertTrue(er.isSuccess());
        assertEquals("MainData|OptData", er.get());

        // 节点状态
        assertEquals(3, er.allNodeStates().size());
        assertTrue(er.getNodeState("main").isSuccess());
        assertTrue(er.getNodeState("opt").isSuccess());
        assertTrue(er.getNodeState("aggregate").isSuccess());

        // 总耗时
        assertTrue(er.getDurationMs() >= 0);

        // Mermaid 包含 optional 样式
        String mermaid = er.getMermaid();
        assertTrue(mermaid.contains("-.->"));
        // 无 failed 样式
        assertFalse(mermaid.contains("classDef failed"));
    }

    // ========== 19. Flow-level timeout ==========

    @Test
    public void testFlowTimeout() {
        TaskNode<String> slow = TaskNode.of("slow", ctx -> {
            Thread.sleep(2000);
            return "done";
        });

        ParallelFlow flow = ParallelFlow.builder()
                .flowTimeout(100)
                .build();

        FlowResult<String> result = flow.tryRun(slow);

        assertFalse(result.isSuccess());
        try {
            result.get();
            fail("Expected ParallelFlowException");
        } catch (ParallelFlowException e) {
            assertTrue(e.getMessage().contains("Flow timed out"));
        }
        assertTrue(result.getDurationMs() >= 80);
        assertTrue(result.getDurationMs() < 2000);
    }

    // ========== 20. ExecutionResult — failure scenario with mermaid ==========

    @Test
    public void testExecuteForResultFailure() {
        TaskNode<String> good = TaskNode.of("good", ctx -> "OK");

        TaskNode<String> bad = TaskNode.of("bad", ctx -> {
            throw new RuntimeException("boom");
        });

        TaskNode<String> optBad = TaskNode.<String>builder("optBad", ctx -> {
            throw new RuntimeException("optional boom");
        }).fallback(ex -> { throw new RuntimeException("fallback also fails"); }).build();

        AtomicBoolean targetRealExecuted = new AtomicBoolean(false);

        TaskNode<String> target = TaskNode.builder("target", ctx -> {
            targetRealExecuted.set(true);
            return good.get();
        }).dependsOn(good, bad).weakDependsOn(optBad).build();

        FlowResult<String> er = ParallelFlow.tryStart(target);

        // 整体失败
        assertFalse(er.isSuccess());
        assertFalse(targetRealExecuted.get());
        try {
            er.get();
            fail("Expected ParallelFlowException");
        } catch (ParallelFlowException ignored) {
        }

        // 各节点状态
        assertTrue(er.getNodeState("good").isSuccess());
        assertFalse(er.getNodeState("bad").isSuccess());

        // Mermaid 包含 failed 和 optionalFailed 样式
        String mermaid = er.getMermaid();
        assertTrue(mermaid.contains("classDef failed"));
        assertTrue(mermaid.contains("class bad failed"));
    }

    @Test
    public void testTimeoutTriggerNext() {

        TaskNode<String> fast = TaskNode.builder("fast", ctx -> {
                    //TimeUnit.MILLISECONDS.sleep(50);
                    return "FastData";
                }).timeout(100)
                .build();

        TaskNode<String> slow = TaskNode.builder("slow", ctx -> {
                    TimeUnit.SECONDS.sleep(30);
                    return "SlowData";
                }).timeout(50)
                .dependsOn(fast)
                .build();



        TaskNode<String> x = TaskNode.builder("x", ctx -> {
                    return slow.orElse("slowDefault") + "|" + fast.orElse("fastDefault");
                }).timeout(TimeUnit.SECONDS.toMillis(1))
                .weakDependsOn(slow, fast)
                .build();


        FlowResult<String> stringFlowResult = ParallelFlow.builder().flowTimeout(TimeUnit.SECONDS.toMillis(3)).build().tryRun(x);
        System.out.println(stringFlowResult.get());
        System.out.println(stringFlowResult.getDurationMs());
    }

}
