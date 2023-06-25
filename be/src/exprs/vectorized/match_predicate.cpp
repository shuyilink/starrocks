#include "exprs/vectorized/match_predicate.h"
#include "column/type_traits.h"
#include "runtime/primitive_type.h"
#include "runtime/primitive_type_infra.h"

namespace starrocks::vectorized {




Expr* VectorizedMatchPredicateFactory::from_thrift(const TExprNode& node) {
    return nullptr;
}

} // starrocks::vectorized namespace