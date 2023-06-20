// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.sql.optimizer.operator.OperatorType;

public class MatchPredicateOperator extends PredicateOperator {

    public MatchPredicateOperator(OperatorType operatorType, ScalarOperator... arguments) {
        super(operatorType, arguments);
    }
}
