#include "storage/rowset/inverted_index_desc.h"
#include "storage/olap_common.h"

#include <fmt/format.h>

#include "gutil/strings/strip.h"

namespace starrocks {

const std::string segment_suffix = ".dat";
const std::string index_suffix = ".idx";
const std::string index_name_separator = "_";

std::string InvertedIndexDescriptor::get_temporary_index_path(const std::string& segment_path,
                                                              uint32_t uuid) {
    return StripSuffixString(segment_path, segment_suffix) + index_name_separator +
           std::to_string(uuid);
}

std::string InvertedIndexDescriptor::get_index_file_name(const std::string& segment_path,
                                                         uint32_t uuid) {
    return StripSuffixString(segment_path, segment_suffix) + index_name_separator +
           std::to_string(uuid) + index_suffix;
}

std::string InvertedIndexDescriptor::inverted_index_file_path(const string& rowset_dir,
                                                              const RowsetId& rowset_id,
                                                              int segment_id, int64_t index_id) {
    // {rowset_dir}/{schema_hash}/{rowset_id}_{seg_num}_{index_id}.idx
    return fmt::format("{}/{}_{}_{}.idx", rowset_dir, rowset_id.to_string(), segment_id, index_id);
}

std::string InvertedIndexDescriptor::local_inverted_index_path_segcompacted(
        const string& tablet_path, const RowsetId& rowset_id, int64_t begin, int64_t end,
        int64_t index_id) {
    // {root_path}/data/{shard_id}/{tablet_id}/{schema_hash}/{rowset_id}_{begin_seg}-{end_seg}_{index_id}.idx
    return fmt::format("{}/{}_{}-{}_{}.idx", tablet_path, rowset_id.to_string(), begin, end,
                       index_id);
}
} // namespace starrocks