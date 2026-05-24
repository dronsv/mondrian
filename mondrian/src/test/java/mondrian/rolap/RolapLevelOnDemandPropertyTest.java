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

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * V1-narrow per dronsv/mondrian#22: schema-author opt-in via the level
 * annotation {@code emondrian.onDemandProperties} partitions
 * {@link RolapLevel#getProperties()} into a projected set (eager) and
 * a skipped set (on-demand).
 *
 * <p>Tests target the static {@link RolapLevel#parseOnDemandAnnotation}
 * and {@link RolapLevel#partitionProperties} helpers — the partitioning
 * logic is pure and easier to unit-test there than through a real
 * cube. The instance-level wiring (annotation lookup, lazy cache,
 * feature-flag gating) is exercised by integration tests under
 * production schemas; covering it in unit tests would require
 * constructing a full {@link RolapLevel} (package-private constructor
 * with 23 args) just to feed an annotation map.
 */
public class RolapLevelOnDemandPropertyTest {

    private RolapProperty mockProperty(String name) {
        RolapProperty p = mock(RolapProperty.class);
        when(p.getName()).thenReturn(name);
        return p;
    }

    @Test
    public void parse_emptySetForNullValue() {
        // Absence of annotation = no opt-in. Treated identically to
        // missing-from-schema, not as "opt in everything".
        assertEquals(Set.of(), RolapLevel.parseOnDemandAnnotation(null));
    }

    @Test
    public void parse_singleName() {
        assertEquals(
            Set.of("URL"),
            RolapLevel.parseOnDemandAnnotation("URL"));
    }

    @Test
    public void parse_csvWithSpacesAndEmptyTokens() {
        // Schema authors will hand-write the CSV; the parser must
        // tolerate whitespace around tokens and accidental empties
        // (trailing commas) rather than silently leaving a property
        // eager.
        assertEquals(
            Set.of("URL", "Claims"),
            RolapLevel.parseOnDemandAnnotation("  URL ,, Claims  , "));
    }

    @Test
    public void parse_resultIsImmutable() {
        Set<String> s = RolapLevel.parseOnDemandAnnotation("A,B");
        assertThrows(
            UnsupportedOperationException.class,
            () -> s.add("C"));
    }

    @Test
    public void partition_emptySkipReturnsInputAsProjectedUnchanged() {
        // Hot path: when no opt-in, the helper must return the *same*
        // array instance for {@code projected}, no copying, no
        // allocation of the skipped list.
        RolapProperty[] all = {
            mockProperty("A"), mockProperty("B")
        };
        RolapLevel.ProjectionPlan plan =
            RolapLevel.partitionProperties(Set.of(), all);
        assertSame(all, plan.projected());
        assertEquals(0, plan.skipped().length);
    }

    @Test
    public void partition_nullInputsAreSafe() {
        // Defensive: if some loader path supplies null, helper must
        // not throw. Empty out-of-both-buckets is the safe answer.
        RolapLevel.ProjectionPlan plan =
            RolapLevel.partitionProperties(Set.of("URL"), null);
        assertEquals(0, plan.projected().length);
        assertEquals(0, plan.skipped().length);
    }

    @Test
    public void partition_skipsOnlyNamedProperties() {
        RolapProperty url = mockProperty("URL");
        RolapProperty brand = mockProperty("Brand");
        RolapProperty claims = mockProperty("Claims");

        RolapLevel.ProjectionPlan plan = RolapLevel.partitionProperties(
            Set.of("URL", "Claims"),
            new RolapProperty[] { url, brand, claims });

        // Projected preserves schema declaration order minus skipped.
        assertArrayEquals(new RolapProperty[] { brand }, plan.projected());
        // Skipped also preserves declaration order so log output is
        // stable and human-readable.
        assertArrayEquals(
            new RolapProperty[] { url, claims }, plan.skipped());
    }

    @Test
    public void partition_namedButNotPresentIsHarmless() {
        // Typo case: annotation lists a name that isn't actually a
        // property. Result is "no skip", quiet. Validation belongs to
        // a separate schema-author-facing diagnostic (out of V1-narrow
        // scope); the engine must not throw.
        RolapProperty brand = mockProperty("Brand");
        RolapLevel.ProjectionPlan plan = RolapLevel.partitionProperties(
            Set.of("Typoed"),
            new RolapProperty[] { brand });
        assertArrayEquals(new RolapProperty[] { brand }, plan.projected());
        assertEquals(0, plan.skipped().length);
    }

    @Test
    public void partition_nullPropertyEntryIsSkippedFromBothBuckets() {
        // Schema arrays in Mondrian can have null slots in degenerate
        // construction (test fixtures and partial builds); the helper
        // must filter them out rather than dispatching on null.name.
        RolapProperty brand = mockProperty("Brand");
        RolapLevel.ProjectionPlan plan = RolapLevel.partitionProperties(
            Set.of("URL"),
            new RolapProperty[] { brand, null });
        assertArrayEquals(new RolapProperty[] { brand }, plan.projected());
        assertEquals(0, plan.skipped().length);
    }

    @Test
    public void annotationConstantHasExpectedName() {
        // The name is the schema-author-visible contract; assert it
        // doesn't drift silently.
        assertEquals(
            "emondrian.onDemandProperties",
            RolapLevel.ON_DEMAND_PROPERTIES_ANNOTATION);
    }
}

// End RolapLevelOnDemandPropertyTest.java
