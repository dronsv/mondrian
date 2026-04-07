/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.*;

/**
 * Query-wide SQL pushdown engine. Replaces Mondrian's cell-by-cell
 * batch-drain loop for eligible queries.
 *
 * <p>Pipeline: Classify (A) -> Resolve (B) -> Merge (C) -> Generate (D).
 * Falls back to legacy evaluator on any unsupported pattern.
 */
public class NativeQueryEngine {
    private static final Logger LOGGER =
        LogManager.getLogger(NativeQueryEngine.class);

    private final Query query;
    private final RolapEvaluator evaluator;
    private final List<MeasureClassifier.Candidate> candidates;

    private NativeQueryEngine(
        Query query,
        RolapEvaluator evaluator,
        List<MeasureClassifier.Candidate> candidates)
    {
        this.query = query;
        this.evaluator = evaluator;
        this.candidates = candidates;
    }

    /**
     * Attempts to create a NativeQueryEngine for the given query.
     * Returns null if the query is not eligible (falls back to legacy).
     */
    public static NativeQueryEngine tryCreate(
        Query query,
        RolapEvaluator evaluator)
    {
        if (!MondrianProperties.instance().NativeQueryEngineEnable.get()) {
            return null;
        }

        final Set<Member> measures = query.getMeasuresMembers();
        if (measures == null || measures.isEmpty()) {
            return null;
        }

        // Phase A: classify
        final List<MeasureClassifier.Candidate> candidates =
            MeasureClassifier.classifyAll(measures);
        if (candidates == null) {
            LOGGER.info(
                "NativeQueryEngine: fallback reason={}, measures={}",
                FallbackReason.UNSUPPORTED_MEASURE_PATTERN,
                measureNames(measures));
            return null;
        }

        LOGGER.info(
            "NativeQueryEngine: eligible, measures={}",
            describeCandidates(candidates));

        return new NativeQueryEngine(query, evaluator, candidates);
    }

    /**
     * Executes the query-wide SQL plan and populates result cells.
     * Returns true if successful, false if fell back.
     */
    public boolean execute(RolapResult result) {
        // TODO: Phase B (DependencyResolver)
        // TODO: Phase C (CoordinateClassMerger)
        // TODO: Phase D (SqlGenerator + PostProcessEvaluator)
        LOGGER.info(
            "NativeQueryEngine: execute() stub — falling back to legacy");
        return false;
    }

    private static String measureNames(Set<Member> measures) {
        StringBuilder sb = new StringBuilder("[");
        int i = 0;
        for (Member m : measures) {
            if (i++ > 0) sb.append(", ");
            sb.append(m.getName());
        }
        return sb.append("]").toString();
    }

    private static String describeCandidates(
        List<MeasureClassifier.Candidate> candidates)
    {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < candidates.size(); i++) {
            if (i > 0) sb.append(", ");
            MeasureClassifier.Candidate c = candidates.get(i);
            sb.append(c.measure.getName())
              .append("=").append(c.candidateClass);
        }
        return sb.append("]").toString();
    }
}

// End NativeQueryEngine.java
