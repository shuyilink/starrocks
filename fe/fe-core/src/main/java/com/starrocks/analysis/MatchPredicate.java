// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.analysis;

import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.thrift.TMatchPredicate;

import java.util.Objects;

public class MatchPredicate extends Predicate {
    public enum Operator {
        MATCH_ANY("MATCH_ANY", "match_any", TExprOpcode.MATCH_ANY);

        private final String description;
        private final String name;
        private final TExprOpcode opcode;

        Operator(String description,
                 String name,
                 TExprOpcode opcode) {
            this.description = description;
            this.name = name;
            this.opcode = opcode;
        }

        @Override
        public String toString() {
            return description;
        }

        public String getName() {
            return name;
        }

        public TExprOpcode getOpcode() {
            return opcode;
        }
    }

    private final Operator op;

    public MatchPredicate(Operator op, Expr e1, Expr e2) {
        this.op = op;
        children.add(e1);
        children.add(e2);
    }

    protected MatchPredicate(MatchPredicate other) {
        super(other);
        op = other.op;
    }

    @Override
    public Expr clone() {
        return new MatchPredicate(this);
    }

    public Operator getOp() {
        return this.op;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return ((MatchPredicate) obj).op == op;
    }

    @Override
    public String toSqlImpl() {
        return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.MATCH_PRED;
        msg.setOpcode(op.getOpcode());
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitMatchPredicate(this, context);
    }
}
