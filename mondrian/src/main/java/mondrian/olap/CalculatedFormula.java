// Copyright (c) 2021 Sergei Semenkov.  All rights reserved.

package mondrian.olap;

import java.io.PrintWriter;

public class CalculatedFormula extends QueryPart {
    private final String cubeName;
    private final Formula e;

    CalculatedFormula(String cubeName, Formula e)
    {
        this.cubeName = cubeName;
        this.e = e;
    }

    @Override
    public void unparse(PrintWriter pw) {
        pw.print("CREATE SESSION MEMBER ");
        pw.print("[" + cubeName + "]");
        pw.print(".");
        if(e != null) {
            pw.print(e.getIdentifier());
            pw.print(" AS ");
            //pw.print(e,getExpression());
        }
        pw.println();
    }

    @Override
    public Object[] getChildren() {
        return new Object[] {e};
    }

    public String getCubeName() {
        return cubeName;
    }

    public Formula getFormula() {
        return e;
    }

}

