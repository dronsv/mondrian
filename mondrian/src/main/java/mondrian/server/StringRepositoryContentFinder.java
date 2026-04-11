/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
* Copyright (C) 2022 Sergei Semenkov
*/

package mondrian.server;

/**
 * Implementation of {@link mondrian.server.RepositoryContentFinder} that always
 * returns a constant string.
 *
 * @author Julian Hyde
 */
public class StringRepositoryContentFinder
    implements RepositoryContentFinder
{
    private final String content;

    public StringRepositoryContentFinder(String content) {
        this.content = content;
    }

    @Override
    public String getContent() {
        return content;
    }

    @Override
    public void setContent(String content) {}

    @Override
    public void shutdown() {
        // nothing to do
    }
}

// End StringRepositoryContentFinder.java
