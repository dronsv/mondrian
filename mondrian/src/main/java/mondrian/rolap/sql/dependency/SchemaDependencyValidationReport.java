/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.sql.dependency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shared validation DTO for schema dependency checks.
 *
 * <p>Designed for reuse by server startup checks, HTTP validation endpoints and
 * future runtime dependency registry compilation diagnostics.</p>
 */
public final class SchemaDependencyValidationReport {
    private final boolean failOnWarn;
    private final List<DependencyRegistry.DependencyValidationIssue> issues =
        new ArrayList<DependencyRegistry.DependencyValidationIssue>();
    private int fatalCount;
    private int warnCount;
    private int infoCount;

    public SchemaDependencyValidationReport(boolean failOnWarn) {
        this.failOnWarn = failOnWarn;
    }

    public boolean isFailOnWarn() {
        return failOnWarn;
    }

    public List<DependencyRegistry.DependencyValidationIssue> getIssues() {
        return Collections.unmodifiableList(issues);
    }

    public int getFatalCount() {
        return fatalCount;
    }

    public int getWarnCount() {
        return warnCount;
    }

    public int getInfoCount() {
        return infoCount;
    }

    public boolean isOk() {
        return fatalCount == 0 && (!failOnWarn || warnCount == 0);
    }

    public void addIssue(DependencyRegistry.DependencyValidationIssue issue) {
        if (issue == null) {
            return;
        }
        issues.add(issue);
        switch (issue.getSeverity()) {
        case FATAL:
            fatalCount++;
            break;
        case WARN:
            warnCount++;
            break;
        default:
            infoCount++;
            break;
        }
    }

    public void merge(SchemaDependencyValidationReport other) {
        if (other == null) {
            return;
        }
        for (DependencyRegistry.DependencyValidationIssue issue : other.issues) {
            addIssue(issue);
        }
    }
}

