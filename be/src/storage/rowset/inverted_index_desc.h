
#pragma once

#include <stdint.h>

#include <string>

namespace starrocks {
struct RowsetId;

class InvertedIndexDescriptor {
public:
    static std::string get_temporary_index_path(const std::string& segment_path, uint32_t uuid);
    static std::string get_index_file_name(const std::string& path, uint32_t uuid);
    static const std::string get_temporary_null_bitmap_file_name() { return "null_bitmap"; }
    static const std::string get_temporary_bkd_index_data_file_name() { return "bkd"; }
    static const std::string get_temporary_bkd_index_meta_file_name() { return "bkd_meta"; }
    static const std::string get_temporary_bkd_index_file_name() { return "bkd_index"; }
    static std::string inverted_index_file_path(const std::string& rowset_dir,
                                                const RowsetId& rowset_id, int segment_id,
                                                int64_t index_id);

    static std::string local_inverted_index_path_segcompacted(const std::string& tablet_path,
                                                              const RowsetId& rowset_id,
                                                              int64_t begin, int64_t end,
                                                              int64_t index_id);
};

} // namespace starrocks