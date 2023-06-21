// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.sql.optimizer.operator.OperatorType;

public class MatchPredicateOperator extends PredicateOperator {

    public MatchPredicateOperator(OperatorType operatorType, ScalarOperator... arguments) {
        super(operatorType, arguments);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getChild(0).toString()).append(" ");
        sb.append("Match Any ");
        sb.append(getChild(1).toString());
        return sb.toString();
    }

    @Override
    public  String debugString() {
        return toString();
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitMatchPredicate(this, context);
    }
}
