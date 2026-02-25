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

import mondrian.olap.Cube;
import mondrian.rolap.RolapCube;

/**
 * Builds {@link DependencyRegistry} from loaded cube metadata.
 *
 * <p>V2 skeleton: only initializes immutable registry shell. The next step is
 * to compile dependency rules and join-path diagnostics here.</p>
 */
public class DependencyRegistryBuilder {
    public DependencyRegistry build(RolapCube cube) {
        final String cubeName = cube == null ? "<unknown-cube>" : cube.getUniqueName();
        DependencyRegistry.Builder builder = DependencyRegistry.builder(cubeName);
        builder.addIssue(new DependencyRegistry.DependencyValidationIssue(
            DependencyRegistry.DependencyValidationSeverity.INFO,
            "DEPENDENCY_REGISTRY_SKELETON",
            "Dependency registry builder skeleton is active; compiled dependency "
                + "descriptors are not populated yet.",
            cubeName,
            null,
            "Implement rule compilation and join-path validation in DependencyRegistryBuilder."));
        return builder.build();
    }

    public DependencyRegistry build(Cube cube) {
        if (cube instanceof RolapCube) {
            return build((RolapCube) cube);
        }
        final String cubeName = cube == null ? "<unknown-cube>" : cube.getUniqueName();
        return DependencyRegistry.builder(cubeName)
            .addIssue(new DependencyRegistry.DependencyValidationIssue(
                DependencyRegistry.DependencyValidationSeverity.WARN,
                "UNSUPPORTED_CUBE_TYPE",
                "Dependency registry is only available for RolapCube in V2 skeleton.",
                cubeName,
                null,
                "Build registry after resolving virtual/base RolapCube metadata."))
            .build();
    }
}

