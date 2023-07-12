#include "storage/rowset/inverted_index_writer.h"

#include "common/logging.h"
#include "common/status.h"
#include "fs/fs.h"
#include "gutil/integral_types.h"
#include "storage/field.h"
#include "storage/inverted_index_parser.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "storage/olap_type_infra.h"
#include "storage/rowset/common.h"
#include "storage/rowset/inverted_index_cache.h"
#include "storage/rowset/inverted_index_compound_directory.h"
#include "storage/rowset/inverted_index_desc.h"
#include "storage/types.h"
#include "storage/type_traits.h"
#include "util/faststring.h"
#include "util/slice.h"

#include <CLucene.h>
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/util/bkd/bkd_writer.h>
#include <roaring/roaring.hh>

#include <cstdint>
#include <string>
#include <string_view>

namespace starrocks {

int32_t MAX_FIELD_LEN = 0x7FFFFFFFL;
int32_t MAX_BUFFER_DOCS = 100000000;
int32_t MERGE_FACTOR = 100000000;
int32_t MAX_LEAF_COUNT = 1024;
float MAXMBSortInHeap = 512.0 * 8;
int32_t DIMS = 1;

std::string_view empty_str = "";

#define FINALIZE_OUTPUT(x) \
    if (x != nullptr) {    \
        x->close();        \
        _CLDELETE(x);      \
    }
#define FINALLY_FINALIZE_OUTPUT(x) \
    try {                          \
        FINALIZE_OUTPUT(x)         \
    } catch (...) {                \
    }

template<FieldType field_type>
class InvertedIndexColumnWriterImpl : public InvertedIndexColumnWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;

    InvertedIndexColumnWriterImpl(std::string field_name, const std::string& seg_filename)
            : _seg_filename(seg_filename), _directory(_seg_filename.parent_path()) {
        _field_name = std::wstring(field_name.begin(), field_name.end());
        _parser_type = InvertedIndexParserType::PARSER_STANDARD;
        _value_key_coder = get_key_coder(field_type);
        _fs = FileSystem::CreateSharedFromString(seg_filename).value();
    }

    Status init() override {
        try {
            if constexpr (is_string_type(field_type)) {
                return init_fulltext_index();
            } else if constexpr (is_numeric_type(field_type)) {
                return init_bkd_index();
            }
            return Status::Unknown("Field type not supported");
        } catch (const CLuceneError& e) {
            LOG(WARNING) << "Inverted index writer init error occurred: " << e.what();
            return Status::Unknown("Inverted index writer init error occurred");
        }
    }

    Status init_bkd_index() {
        size_t value_length = sizeof(CppType);
        // NOTE: initialize with 0, set to max_row_id when finished.
        int32_t max_doc = 0;
        int64_t total_point_count = std::numeric_limits<std::int32_t>::max();
        _bkd_writer = std::make_shared<lucene::util::bkd::bkd_writer>(
                max_doc, DIMS, DIMS, value_length, MAX_LEAF_COUNT, MAXMBSortInHeap,
                total_point_count, true, config::max_depth_in_bkd_tree);
        return Status::OK();
    }

    Status init_fulltext_index() {
        bool create = true;

        auto index_path =
            InvertedIndexDescriptor::get_temporary_index_path(_seg_filename.c_str(), 0);

        if (lucene::index::IndexReader::indexExists(index_path.c_str())) {
            create = false;
            if (lucene::index::IndexReader::isLocked(index_path.c_str())) {
                LOG(WARNING) << ("Lucene Index was locked... unlocking it.\n");
                lucene::index::IndexReader::unlock(index_path.c_str());
            }
        }

        _char_string_reader = std::make_unique<lucene::util::SStringReader<char>>();
        _doc = std::make_unique<lucene::document::Document>();
        _dir.reset(CompoundDirectory::getDirectory(_fs.get(), index_path.c_str(), true));
        if (_parser_type == InvertedIndexParserType::PARSER_STANDARD) {
            _analyzer = std::make_unique<lucene::analysis::standard::StandardAnalyzer>();
        } else if (_parser_type == InvertedIndexParserType::PARSER_ENGLISH) {
            _analyzer = std::make_unique<lucene::analysis::SimpleAnalyzer<char>>();
        } else if (_parser_type == InvertedIndexParserType::PARSER_CHINESE) {
            // auto chinese_analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer();
            // chinese_analyzer->setLanguage(L"chinese");
            // chinese_analyzer->initDict(config::inverted_index_dict_path);
            // auto mode = get_parser_mode_string_from_properties(_index_meta->properties());
            // if (mode == INVERTED_INDEX_PARSER_COARSE_GRANULARITY) {
            //     chinese_analyzer->setMode(lucene::analysis::AnalyzerMode::Default);
            // } else {
            //     chinese_analyzer->setMode(lucene::analysis::AnalyzerMode::All);
            // }
            // _analyzer.reset(chinese_analyzer);
        } else {
            // ANALYSER_NOT_SET, ANALYSER_NONE use default SimpleAnalyzer
            _analyzer = std::make_unique<lucene::analysis::SimpleAnalyzer<TCHAR>>();
        }
        _index_writer = std::make_unique<lucene::index::IndexWriter>(_dir.get(), _analyzer.get(), create, true);
        _index_writer->setMaxBufferedDocs(MAX_BUFFER_DOCS);
        _index_writer->setRAMBufferSizeMB(config::inverted_index_ram_buffer_size);
        _index_writer->setMaxFieldLength(MAX_FIELD_LEN);
        _index_writer->setMergeFactor(MERGE_FACTOR);
        _index_writer->setUseCompoundFile(false);

        auto field_config = lucene::document::Field::STORE_NO |
                            lucene::document::Field::INDEX_NONORMS;
        if (_parser_type == InvertedIndexParserType::PARSER_NONE) {
            field_config |= lucene::document::Field::INDEX_UNTOKENIZED;
        } else {
            field_config |= lucene::document::Field::INDEX_TOKENIZED;
        }
        _field = std::make_unique<lucene::document::Field>(_field_name.c_str(), field_config);
        // if (get_parser_phrase_support_string_from_properties(_index_meta->properties()) ==
        //     INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES) {
        //     _field->setOmitTermFreqAndPositions(false);
        // } else {
            // _field->setOmitTermFreqAndPositions(true);
        // }
        _doc->add(*_field);
        return Status::OK();
    }

    void close() {
        if (_index_writer) {
            _index_writer->close();
            if (config::enable_write_index_searcher_cache) {
                // open index searcher into cache
                auto index_file_name 
                    = InvertedIndexDescriptor::get_index_file_name(_seg_filename, index_id);
                InvertedIndexSearcherCache::instance()->insert(_fs.get(), _directory, index_file_name);
            }
        }
    }

    void add_value(const CppType& value) {
        std::string new_value;
        size_t value_length = sizeof(CppType);

        _value_key_coder->full_encode_ascending(&value, &new_value);
        _bkd_writer->add((const uint8_t*)new_value.c_str(), value_length, _rid);

        _rid++;
    }

    void add_numeric_values(const void* values, size_t count) {
        auto p = reinterpret_cast<const CppType*>(values);
        for (size_t i = 0; i < count; ++i) {
            add_value(*p);
            p++;
            _row_ids_seen_for_bkd++;
        }
    }

    Status add_values(const void* values, size_t count) override {
        if constexpr (is_string_type(field_type)) {
            RETURN_IF_UNLIKELY(_field == nullptr || _index_writer == nullptr,
                Status::InternalError("InvertedIndex: field or index writer is null"));

            auto v = (Slice*)values;
            for (int i = 0; i < count; ++i) {
                new_fulltext_field(v->get_data(), v->get_size());
                _index_writer->addDocument(_doc.get());
                ++v;
                _rid++;
            }
        } else if constexpr (is_numeric_type(field_type)) {
            add_numeric_values(values, count);
        }
        return Status::OK();
    }

    void new_char_token_stream(const char* s, size_t len, lucene::document::Field* field) {
        _char_string_reader->init(s, len, false);
        auto stream = _analyzer->reusableTokenStream(field->name(), _char_string_reader.get());
        field->setValue(stream);
    }

    void new_field_value(const char* s, size_t len, lucene::document::Field* field) {
        auto field_value = lucene::util::Misc::_charToWide(s, len);
        field->setValue(field_value, false);
        // setValue did not duplicate value, so we don't have to delete
        //_CLDELETE_ARRAY(field_value)
    }

    void new_fulltext_field(const char* field_value_data, size_t field_value_size) {
        if (_parser_type == InvertedIndexParserType::PARSER_ENGLISH ||
            _parser_type == InvertedIndexParserType::PARSER_CHINESE) {
            new_char_token_stream(field_value_data, field_value_size, _field.get());
        } else {
            new_field_value(field_value_data, field_value_size, _field.get());
        }
    }

    Status add_nulls(uint32_t count) override {
        _null_bitmap.addRange(_rid, _rid + count);
        _rid += count;
        if constexpr (is_string_type(field_type)) {
            RETURN_IF_UNLIKELY(_field == nullptr || _index_writer == nullptr,
                Status::InternalError("InvertedIndex: field or index writer is null"));

            for (int32_t idx = 0; idx < count; ++idx) {
               new_fulltext_field(empty_str.data(), empty_str.size());
               _index_writer->addDocument(_doc.get());
            }
        }
        return Status::OK();
    }

    void write_null_bitmap(lucene::store::IndexOutput* null_bitmap_out,
                           lucene::store::Directory* dir) {
        // write null_bitmap file
        _null_bitmap.runOptimize();
        size_t size = _null_bitmap.getSizeInBytes(false);
        if (size > 0) {
            null_bitmap_out = dir->createOutput(
                    InvertedIndexDescriptor::get_temporary_null_bitmap_file_name().c_str());
            faststring buf;
            buf.resize(size);
            _null_bitmap.write(reinterpret_cast<char*>(buf.data()), false);
            null_bitmap_out->writeBytes(reinterpret_cast<uint8_t*>(buf.data()), size);
            FINALIZE_OUTPUT(null_bitmap_out)
        }
    }


    Status finish() override {
        lucene::store::Directory* dir = nullptr;
        lucene::store::IndexOutput* null_bitmap_out = nullptr;
        lucene::store::IndexOutput* data_out = nullptr;
        lucene::store::IndexOutput* index_out = nullptr;
        lucene::store::IndexOutput* meta_out = nullptr;
        try {
            // write bkd file
            if constexpr (is_numeric_type(field_type)) {
                auto index_path = InvertedIndexDescriptor::get_temporary_index_path(
                        _seg_filename.string(), index_id);
                dir = CompoundDirectory::getDirectory(_fs.get(), index_path.c_str(), true);
                write_null_bitmap(null_bitmap_out, dir);
                _bkd_writer->max_doc_ = _rid;
                _bkd_writer->docs_seen_ = _row_ids_seen_for_bkd;
                data_out = dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_bkd_index_data_file_name().c_str());
                meta_out = dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_bkd_index_meta_file_name().c_str());
                index_out = dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_bkd_index_file_name().c_str());
                if (data_out != nullptr && meta_out != nullptr && index_out != nullptr) {
                    _bkd_writer->meta_finish(meta_out, _bkd_writer->finish(data_out, index_out),
                                             int(field_type));
                }
                FINALIZE_OUTPUT(meta_out)
                FINALIZE_OUTPUT(data_out)
                FINALIZE_OUTPUT(index_out)
                FINALIZE_OUTPUT(dir)
            } else if constexpr (is_string_type(field_type)) {
                dir = _index_writer->getDirectory();
                write_null_bitmap(null_bitmap_out, dir);
                this->close();
            }
        } catch (CLuceneError& e) {
            FINALLY_FINALIZE_OUTPUT(null_bitmap_out)
            FINALLY_FINALIZE_OUTPUT(meta_out)
            FINALLY_FINALIZE_OUTPUT(data_out)
            FINALLY_FINALIZE_OUTPUT(index_out)
            FINALLY_FINALIZE_OUTPUT(dir)
            LOG(WARNING) << "InvertedIndex: fail to finish(), error:" << e.what();
            return Status::InternalError(
                "Inverted index writer finish error occurred");
        }

        return Status::OK();
    }

    int64_t size() const override {
        return sizeof(InvertedIndexColumnWriterImpl<field_type>);
    }

    int64_t file_size() const override {
        auto file_name =
                InvertedIndexDescriptor::get_index_file_name(_seg_filename.string(), 0);
        if (auto ret = _fs->get_file_size(file_name.c_str()); ret.ok()) {
            return ret.value();
        }
        return -1;
    }
private:
    rowid_t _rid = 0;
    uint32_t _row_ids_seen_for_bkd = 0;
    int32_t index_id = 0; // TODO:
    Roaring _null_bitmap;

    std::wstring _field_name;
    std::filesystem::path _seg_filename;
    std::filesystem::path _directory;
    std::shared_ptr<FileSystem> _fs;

    const KeyCoder* _value_key_coder;
    InvertedIndexParserType _parser_type;

    std::unique_ptr<lucene::document::Field> _field;
    std::unique_ptr<lucene::document::Document> _doc;
    std::unique_ptr<lucene::index::IndexWriter> _index_writer;
    std::unique_ptr<lucene::analysis::Analyzer> _analyzer;
    std::unique_ptr<lucene::util::SStringReader<char>> _char_string_reader;
    std::shared_ptr<lucene::util::bkd::bkd_writer> _bkd_writer;
    std::unique_ptr<CompoundDirectory> _dir;
};

struct InvertedIndexWriterBuilder {
    template <FieldType ftype>
    std::unique_ptr<InvertedIndexColumnWriter> operator()(std::string field_name, const std::string& seg_filename) {
        return std::make_unique<InvertedIndexColumnWriterImpl<ftype>>(field_name, seg_filename);
    }
};

Status InvertedIndexColumnWriter::create(const Field* field, const std::string& seg_filename,
                                         std::unique_ptr<InvertedIndexColumnWriter>* res) {
    *res = field_type_dispatch_inverted_index(
                field->type_info()->type(), InvertedIndexWriterBuilder(), field->name(), seg_filename);
    return Status::OK();
}

} // namespace starrocks