/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (c) 2021-2022 Sergei Semenkov.
 * All rights reserved.
 */

package mondrian.xmla.impl;

import org.olap4j.metadata.XmlaConstants;

import java.util.Map;
import java.util.HashMap;
import mondrian.xmla.XmlaRequest;

public class DmvXmlaRequest
        implements XmlaRequest
{
    private HashMap<String, Object> restrictions;
    private HashMap<String, String> properties;
    private String roleName;
    private String requestType;
    private String username;
    private String password;
    private String sessionId;

    public DmvXmlaRequest(
            Map<String, Object> restrictions,
            Map<String, String> properties,
            String roleName,
            String requestType,
            String username,
            String password,
            String sessionId
    )
    {
        this.restrictions = new HashMap<>(restrictions);
        this.properties = new HashMap<>(properties);
        this.roleName = roleName;
        this.requestType = requestType;
        this.username = username;
        this.password = password;
        this.sessionId = sessionId;
    }

    @Override public XmlaConstants.Method getMethod() { return XmlaConstants.Method.DISCOVER; }

    @Override public Map<String, String> getProperties() { return new HashMap<>(); }

    @Override public Map<String, Object> getRestrictions() { return this.restrictions; }

    @Override public String getStatement() { return null; }

    @Override public String getRoleName() { return this.roleName; }

    @Override public String getRequestType() { return this.requestType; }

    @Override public boolean isDrillThrough() { return false; }

    @Override public String getUsername() { return this.username; }

    @Override public String getPassword() { return this.password; }

    @Override public String getSessionId() { return this.sessionId; }

    @Override public String getAuthenticatedUser() {
        return null;
    }

    @Override public String[] getAuthenticatedUserGroups() {
        return null;
    }
}
