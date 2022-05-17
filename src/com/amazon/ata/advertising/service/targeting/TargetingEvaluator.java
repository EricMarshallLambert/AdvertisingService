package com.amazon.ata.advertising.service.targeting;

import com.amazon.ata.advertising.service.model.RequestContext;
import com.amazon.ata.advertising.service.targeting.predicate.TargetingPredicate;
import com.amazon.ata.advertising.service.targeting.predicate.TargetingPredicateResult;
import org.apache.logging.log4j.core.util.ExecutorServices;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Evaluates TargetingPredicates for a given RequestContext.
 */
public class TargetingEvaluator {
    public static final boolean IMPLEMENTED_STREAMS = true;
    public static final boolean IMPLEMENTED_CONCURRENCY = true;
    private final RequestContext requestContext;

    /**
     * Creates an evaluator for targeting predicates.
     * @param requestContext Context that can be used to evaluate the predicates.
     */
    public TargetingEvaluator(RequestContext requestContext) {
        this.requestContext = requestContext;
    }

    /**
     * Evaluate a TargetingGroup to determine if all of its TargetingPredicates are TRUE or not for the given
     * RequestContext.
     * @param targetingGroup Targeting group for an advertisement, including TargetingPredicates.
     * @return TRUE if all of the TargetingPredicates evaluate to TRUE against the RequestContext, FALSE otherwise.
     */
    public TargetingPredicateResult evaluate(TargetingGroup targetingGroup) {
        ExecutorService executorService = Executors.newCachedThreadPool();

        List<Future<TargetingPredicateResult>> targetingPredicateResultFutures =targetingGroup.getTargetingPredicates()
                .stream()
                .map(targetingPredicate -> executorService.submit(()->targetingPredicate.evaluate(requestContext)))
                .collect(Collectors.toList());
        executorService.shutdown();

        return targetingPredicateResultFutures
                .stream()
                .map(targetingPredicateResultFuture -> {
                    try {
                        return targetingPredicateResultFuture.get(1000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException | TimeoutException | ExecutionException e) {
                        e.printStackTrace();
                        return TargetingPredicateResult.FALSE;
                    }
                })
                .allMatch(TargetingPredicateResult::isTrue)? TargetingPredicateResult.TRUE :
                                   TargetingPredicateResult.FALSE;
    }
}
