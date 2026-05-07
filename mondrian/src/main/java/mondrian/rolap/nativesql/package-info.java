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
 * Native SQL work registry and shared substrate primitives.
 *
 * <p>Two coexisting planes share this package:
 * <ul>
 *   <li><b>Pending plane</b> — per-{@code RolapEvaluatorRoot} queue +
 *       drain loop. Used by {@code NativeSqlCalc} and
 *       {@code NativeQuerySqlGenerator} Phase D. Sentinel re-entry via
 *       {@code valueNotReadyException}.</li>
 *   <li><b>One-shot plane</b> — synchronous static entry point on
 *       {@link mondrian.rolap.nativesql.NativeSqlRegistry#executeOneShot}.
 *       Used by selected {@code SqlMemberSource} call sites. No
 *       {@code Locus} or {@code RolapEvaluatorRoot} dependency.</li>
 * </ul>
 *
 * <p>See docs/superpowers/specs/2026-04-10-cell-phase-native-registry-design.md
 * for the original cell-phase / pending-plane contracts and
 * docs/superpowers/specs/2026-05-07-phase-8a-sqlmembersource-registry-consumer-design.md
 * for the Phase 8a rename and one-shot plane design.
 */
package mondrian.rolap.nativesql;
