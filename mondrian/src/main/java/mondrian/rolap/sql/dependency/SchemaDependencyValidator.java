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

import java.io.File;

/**
 * Shared validator contract for dependency-related schema diagnostics.
 */
public interface SchemaDependencyValidator {
    SchemaDependencyValidationReport validateDirectory(
        File schemaDir,
        boolean failOnWarn);

    SchemaDependencyValidationReport validateSchemaXml(
        String schemaXml,
        String schemaName,
        boolean failOnWarn);
}

