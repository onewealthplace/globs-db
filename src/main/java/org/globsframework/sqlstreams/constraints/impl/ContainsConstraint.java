package org.globsframework.sqlstreams.constraints.impl;

import org.globsframework.metamodel.Field;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.ConstraintVisitor;

public class ContainsConstraint implements Constraint {
    public final Field field;
    public final String value;

    public ContainsConstraint(Field field, String value) {
        this.field = field;
        this.value = value;
    }

    public void visit(ConstraintVisitor constraintVisitor) {
        constraintVisitor.visitContains(field, value);
    }

}
