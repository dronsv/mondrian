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

import mondrian.olap.Util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
* Implementation of {@link mondrian.server.RepositoryContentFinder} that reads
 * from a URL.
 *
 * <p>The URL might be a string representation of a {@link java.net.URL}, such
 * as 'file:/foo/bar/datasources.xml' or 'http://server/datasources.xml', but
 * it might also be the mondrian-specific URL format 'inline:...'. The content
 * of an inline URL is the rest of the string after the 'inline:' prefix.
 *
 * @author Julian Hyde
*/
public class UrlRepositoryContentFinder
    implements RepositoryContentFinder
{
    protected final String url;

    /**
     * Creates a UrlRepositoryContentFinder.
     *
     * @param url URL of repository
     */
    public UrlRepositoryContentFinder(String url) {
        assert url != null;
        this.url = url;
    }

    public String getContent() {
        try {
            return Util.readURL(
                url, Util.toMap(System.getProperties()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setContent(String content) {
        try {
            String filePath = java.net.URI.create(url).getPath();
            java.io.BufferedWriter out = new java.io.BufferedWriter(new java.io.FileWriter(filePath, StandardCharsets.UTF_8));
            try {
                out.write(content);
            } finally {
                out.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        // nothing to do
    }
}

// End UrlRepositoryContentFinder.java
