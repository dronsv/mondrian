/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.olap;

/**
 * <code>DelegatingRole</code> implements {@link Role} by
 * delegating all methods to an underlying {@link Role}.
 *
 * <p>This is a convenient base class if you want to override just a few of
 * {@link Role}'s methods.
 *
 * @author Richard M. Emberson
 * @since Mar 29 2007
 */
public class DelegatingRole implements Role {
    protected final Role role;

    /**
     * Creates a DelegatingRole.
     *
     * @param role Underlying role.
     */
    public DelegatingRole(Role role) {
        assert role != null;
        this.role = role;
    }

    @Override
    public Access getAccess(Schema schema) {
        return role.getAccess(schema);
    }

    @Override
    public Access getAccess(Cube cube) {
        return role.getAccess(cube);
    }

    @Override
    public Access getAccess(Dimension dimension) {
        return role.getAccess(dimension);
    }

    @Override
    public Access getAccess(Hierarchy hierarchy) {
        return role.getAccess(hierarchy);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation returns the same access as the underlying role.
     * Derived class may choose to refine access by creating a subclass of
     * {@link mondrian.olap.RoleImpl.DelegatingHierarchyAccess}.
     */
    @Override
    public HierarchyAccess getAccessDetails(Hierarchy hierarchy) {
        return role.getAccessDetails(hierarchy);
    }

    @Override
    public Access getAccess(Level level) {
        return role.getAccess(level);
    }

    @Override
    public Access getAccess(Member member) {
        return role.getAccess(member);
    }

    @Override
    public Access getAccess(NamedSet set) {
        return role.getAccess(set);
    }

    @Override
    public boolean canAccess(OlapElement olapElement) {
        return role.canAccess(olapElement);
    }
}

// End DelegatingRole.java
