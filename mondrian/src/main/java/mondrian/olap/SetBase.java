/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// Copyright (C) 2022 Sergei Semenkov
// All Rights Reserved.
*/

package mondrian.olap;

import mondrian.olap.type.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Skeleton implementation of {@link NamedSet} interface.
 *
 * @author jhyde
 * @since 6 August, 2001
 */
public class SetBase extends OlapElementBase implements NamedSet {

    private static final Logger LOGGER = LogManager.getLogger(SetBase.class);

    private String name;
    private Map<String, Annotation> annotationMap;
    private String description;
    private final String uniqueName;
    private Exp exp;
    private boolean validated;
    private String displayFolder;

    /**
     * Creates a SetBase.
     *
     * @param name Name
     * @param caption Caption
     * @param description Description
     * @param exp Expression
     * @param validated Whether has been validated
     * @param annotationMap Annotations
     */
    SetBase(
        String name,
        String caption,
        String description,
        Exp exp,
        boolean validated,
        Map<String, Annotation> annotationMap)
    {
        this.name = name;
        this.annotationMap = annotationMap;
        this.caption = caption;
        this.description = description;
        this.exp = exp;
        this.validated = validated;
        this.uniqueName = "[" + name + "]";
    }

    @Override public Map<String, Annotation> getAnnotationMap() {
        return annotationMap;
    }

    @Override public String getNameUniqueWithinQuery() {
        return System.identityHashCode(this) + "";
    }

    @Override public boolean isDynamic() {
        return false;
    }

    @Override public Object clone() {
        return new SetBase(
            name, caption, description, exp.clone(), validated, annotationMap);
    }

    @Override protected Logger getLogger() {
        return LOGGER;
    }

    @Override public String getUniqueName() {
        return uniqueName;
    }

    @Override public String getName() {
        return name;
    }

    @Override public String getQualifiedName() {
        return null;
    }

    @Override public String getDescription() {
        return description;
    }

    public String getDisplayFolder() {
        return displayFolder;
    }

    public List<Hierarchy> getHierarchies() {
        return ((SetType)exp.getType()).getHierarchies();
    }

    @Override public Hierarchy getHierarchy() {
        return exp.getType().getHierarchy();
    }

    @Override public Dimension getDimension() {
        return getHierarchy().getDimension();
    }

    @Override public OlapElement lookupChild(
        SchemaReader schemaReader, Id.Segment s, MatchType matchType)
    {
        return null;
    }

    @Override public void setName(String name) {
        this.name = name;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setDisplayFolder(String displayFolder) {
        this.displayFolder = displayFolder;
    }

    public void setAnnotationMap(Map<String, Annotation> annotationMap) {
        this.annotationMap = annotationMap;
    }

    @Override public Exp getExp() {
        return exp;
    }

    @Override public NamedSet validate(Validator validator) {
        if (!validated) {
            exp = validator.validate(exp, false);
            validated = true;
        }
        return this;
    }

    @Override public Type getType() {
        Type type = exp.getType();
        if (type instanceof MemberType
            || type instanceof TupleType)
        {
            // You can use a member or tuple as the expression for a set. It is
            // implicitly converted to a set. The expression may not have been
            // converted yet, so we wrap the type here.
            type = new SetType(type);
        }
        return type;
    }
}

// End SetBase.java
