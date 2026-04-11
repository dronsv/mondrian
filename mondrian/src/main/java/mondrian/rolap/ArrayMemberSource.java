/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
//
// jhyde, 22 December, 2001
*/

package mondrian.rolap;

import mondrian.olap.Id;
import mondrian.olap.Util;
import mondrian.resource.MondrianResource;

import java.util.Collections;
import java.util.List;

/**
 * <code>ArrayMemberSource</code> implements a flat, static hierarchy. There is
 * no root member, and all members are siblings.
 *
 * @author jhyde
 * @since 22 December, 2001
 */
abstract class ArrayMemberSource implements MemberSource {

    protected final RolapHierarchy hierarchy;
    protected final List<RolapMember> members;

    ArrayMemberSource(RolapHierarchy hierarchy, List<RolapMember> members) {
        this.hierarchy = hierarchy;
        this.members = members;
    }
    @Override
    public RolapHierarchy getHierarchy() {
        return hierarchy;
    }
    @Override
    public boolean setCache(MemberCache cache) {
        return false; // we do not support cache writeback
    }
    @Override
    public List<RolapMember> getMembers() {
        return members;
    }
    @Override
    public int getMemberCount() {
        return members.size();
    }

    @Override
    public List<RolapMember> getRootMembers() {
        return Collections.emptyList();
    }

    @Override
    public void getMemberChildren(
        RolapMember parentMember,
        List<RolapMember> children)
    {
        // there are no children
    }

    @Override
    public void getMemberChildren(
        List<RolapMember> parentMembers,
        List<RolapMember> children)
    {
        // there are no children
    }

    @Override
    public RolapMember lookupMember(
        List<Id.Segment> uniqueNameParts,
        boolean failIfNotFound)
    {
        String uniqueName = Util.implode(uniqueNameParts);
        for (RolapMember member : members) {
            if (member.getUniqueName().equals(uniqueName)) {
                return member;
            }
        }
        if (failIfNotFound) {
            throw MondrianResource.instance().MdxCantFindMember.ex(uniqueName);
        } else {
            return null;
        }
    }
}

// End ArrayMemberSource.java
