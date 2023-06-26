#include "storage/rowset/inverted_index_writer.h"
#include "storage/types.h"

#include <CLucene.h>
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/util/bkd/bkd_writer.h>

namespace starrocks {

Status InvertedIndexColumnWriter::create(const TypeInfoPtr& type_info, std::unique_ptr<InvertedIndexColumnWriter>* res) {
    return Status::OK();
}

} // namespace starrocks