#pragma once

#include "common/status.h"
#include "fs/fs.h"
#include "gutil/integral_types.h"

#include "storage/inverted_index_parser.h"
#include "storage/rowset/inverted_index_compound_reader.h"
#include "storage/tablet_schema.h"

#include <CLucene/util/bkd/bkd_reader.h>
#include <stdint.h>
#include "roaring/roaring.hh"

#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace lucene {
namespace store { 
class Directory;
}
// namespace util::bkd {
// class bkd_docid_set_iterator;
// }
}

namespace roaring {
class Roaring;
}

namespace starrocks {
class KeyCoder;
class TypeInfo;
struct OlapReaderStatistics;

class InvertedIndexIterator;
class InvertedIndexQueryCacheHandle;

enum class InvertedIndexReaderType {
    UNKNOWN = -1,
    FULLTEXT = 0,
    STRING_TYPE = 1,
    BKD = 2,
};

enum class InvertedIndexQueryType {
    UNKNOWN_QUERY = -1,
    EQUAL_QUERY = 0,
    LESS_THAN_QUERY = 1,
    LESS_EQUAL_QUERY = 2,
    GREATER_THAN_QUERY = 3,
    GREATER_EQUAL_QUERY = 4,
    MATCH_ANY_QUERY = 5,
    MATCH_ALL_QUERY = 6,
    MATCH_PHRASE_QUERY = 7,
};

class InvertedIndexReader {
public:
    explicit InvertedIndexReader(FileSystem* fs, const std::string& path)
            : _fs(fs), _path(path) {}
    virtual ~InvertedIndexReader() = default;

    // create a new column iterator. Client should delete returned iterator
    virtual Status new_iterator(OlapReaderStatistics* stats, InvertedIndexIterator** iterator) = 0;

    virtual Status query(OlapReaderStatistics* stats, const std::string& column_name,
                         const void* query_value, InvertedIndexQueryType query_type,
                         Roaring* bit_map) = 0;
    virtual Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                             const void* query_value, InvertedIndexQueryType query_type,
                             uint32_t* count) = 0;

    Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr);

    virtual InvertedIndexReaderType type() = 0;
    bool indexExists(const std::string& file_path);

    uint32_t get_index_id() const { return 0; }

    // const std::map<string, string>& get_index_properties() const {
    //     return _index_meta.properties();
    // }

    static std::vector<std::wstring> get_analyse_result(const std::string& field_name,
                                                        const std::string& value,
                                                        InvertedIndexQueryType query_type,
                                                        InvertedIndexCtx* inverted_index_ctx);

protected:
    bool _is_range_query(InvertedIndexQueryType query_type);
    bool _is_match_query(InvertedIndexQueryType query_type);
    friend class InvertedIndexIterator;
    FileSystem* _fs;
    std::filesystem::path _path;
    const int32_t index_id = 0;
    // TabletIndex _index_meta;
};

class FullTextIndexReader : public InvertedIndexReader {
public:
    explicit FullTextIndexReader(FileSystem* fs, const std::string& path)
            : InvertedIndexReader(std::move(fs), path) {}

    Status new_iterator(OlapReaderStatistics* stats, InvertedIndexIterator** iterator) override;

    Status query(OlapReaderStatistics* stats, const std::string& column_name,
                 const void* query_value, InvertedIndexQueryType query_type,
                 Roaring* bit_map) override;

    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     const void* query_value, InvertedIndexQueryType query_type,
                     uint32_t* count) override {
        return Status::InternalError("unimplement");
    }

    InvertedIndexReaderType type() override;
};

class StringTypeInvertedIndexReader : public InvertedIndexReader {
public:
    explicit StringTypeInvertedIndexReader(FileSystem* fs, const std::string& path)
            : InvertedIndexReader(std::move(fs), path) { }

    Status new_iterator(OlapReaderStatistics* stats, InvertedIndexIterator** iterator) override;

    Status query(OlapReaderStatistics* stats, const std::string& column_name,
                 const void* query_value, InvertedIndexQueryType query_type,
                 Roaring* bit_map) override;

    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     const void* query_value, InvertedIndexQueryType query_type,
                     uint32_t* count) override {
        return Status::InternalError("unimplement");
    }

    InvertedIndexReaderType type() override;
};

class InvertedIndexVisitor : public lucene::util::bkd::bkd_reader::intersect_visitor {
private:
    Roaring* _hits;
    uint32_t _num_hits;
    bool _only_count;
    lucene::util::bkd::bkd_reader* _reader;
    InvertedIndexQueryType _query_type;

public:
    std::string query_min;
    std::string query_max;

    InvertedIndexVisitor(Roaring* hits, InvertedIndexQueryType query_type,
                         bool only_count = false);

    void set_reader(lucene::util::bkd::bkd_reader* r) { _reader = r; }
    lucene::util::bkd::bkd_reader* get_reader() { return _reader; }

    void visit(int row_id) override;
    void visit(Roaring &) override;
    void visit(Roaring&& r) override;
    void visit(Roaring* doc_id, std::vector<uint8_t>& packed_value) override;
    void visit(std::vector<char>& doc_id, std::vector<uint8_t>& packed_value) override;
    void visit(int row_id, std::vector<uint8_t>& packed_value) override;
    void visit(lucene::util::bkd::bkd_docid_set_iterator* iter,
               std::vector<uint8_t>& packed_value) override;

    bool matches(uint8_t* packed_value);
    lucene::util::bkd::relation compare(std::vector<uint8_t>& min_packed,
                                        std::vector<uint8_t>& max_packed) override;
    uint32_t get_num_hits() const { return _num_hits; }
};

class BkdIndexReader : public InvertedIndexReader {
public:
    explicit BkdIndexReader(FileSystem* fs, const std::string& path);
    ~BkdIndexReader() override;

    Status new_iterator(OlapReaderStatistics* stats, InvertedIndexIterator** iterator) override;

    Status query(OlapReaderStatistics* stats, const std::string& column_name,
                 const void* query_value, InvertedIndexQueryType query_type,
                Roaring* bit_map) override;
    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     const void* query_value, InvertedIndexQueryType query_type,
                     uint32_t* count) override;
    Status bkd_query(OlapReaderStatistics* stats, const std::string& column_name,
                     const void* query_value, InvertedIndexQueryType query_type,
                     std::shared_ptr<lucene::util::bkd::bkd_reader>& r,
                     InvertedIndexVisitor* visitor);

    InvertedIndexReaderType type() override;
    Status get_bkd_reader(std::shared_ptr<lucene::util::bkd::bkd_reader>& reader);

private:
    const TypeInfo* _type_info;
    const KeyCoder* _value_key_coder;
    CompoundReader* _compoundReader;
};

class InvertedIndexIterator {
public:
    InvertedIndexIterator(OlapReaderStatistics* stats, InvertedIndexReader* reader)
            : _stats(stats), _reader(reader) {}

    Status read_from_inverted_index(const std::string& column_name, const void* query_value,
                                    InvertedIndexQueryType query_type, uint32_t segment_num_rows,
                                    Roaring* bit_map, bool skip_try = false);
    Status try_read_from_inverted_index(const std::string& column_name, const void* query_value,
                                        InvertedIndexQueryType query_type, uint32_t* count);

    Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr) {
        return _reader->read_null_bitmap(cache_handle, dir);
    }

    InvertedIndexReaderType get_inverted_index_reader_type() const;
    // const std::map<string, string>& get_index_properties() const;

private:
    OlapReaderStatistics* _stats;
    InvertedIndexReader* _reader;
};

} // namespace starrocks
