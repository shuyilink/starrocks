#include "storage/rowset/inverted_index_writer.h"
#include "common/status.h"
#include "storage/types.h"

namespace starrocks {

Status InvertedIndexColumnWriter::create(const TypeInfoPtr& type_info, std::unique_ptr<InvertedIndexColumnWriter>* res) {
    return Status::OK();
}

} // namespace starrocks