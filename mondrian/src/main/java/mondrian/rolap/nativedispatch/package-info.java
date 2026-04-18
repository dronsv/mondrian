/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
/**
 * Semantic shape value objects for native-dispatch rule evaluation.
 *
 * <p>This package contains the hierarchy-keyed query shape model used
 * by native-dispatch rules.  The types here are immutable value objects
 * that capture the semantic state of how each hierarchy participates in
 * a query (constrained? projected? what cardinality? what level?), so
 * that rules stay declarative and never need to re-inspect raw MDX.
 *
 * <p><b>Foundation layer.</b>  This package provides the semantic shape
 * model only.  Query-shape extraction (analyzer), dispatch rule
 * evaluation, and pipeline integration are planned for follow-up work.
 *
 * @see mondrian.rolap.nativedispatch.NativeQueryShape
 * @see mondrian.rolap.nativedispatch.HierarchyPresence
 */
package mondrian.rolap.nativedispatch;
