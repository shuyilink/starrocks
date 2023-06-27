#include "storage/rowset/inverted_index_writer.h"

#include "common/logging.h"
#include "fs/fs.h"
#include "gutil/integral_types.h"
#include "storage/field.h"
#include "storage/inverted_index_parser.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "storage/olap_type_infra.h"
#include "storage/rowset/common.h"
#include "storage/rowset/inverted_index_desc.h"
#include "storage/rowset/inverted_index_compound_directory.h"
#include "storage/types.h"
#include "storage/type_traits.h"

#include <CLucene.h>
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/util/bkd/bkd_writer.h>
#include <cstdint>
#include <roaring/roaring.hh>
#include <string>

namespace starrocks {

int32_t MAX_FIELD_LEN = 0x7FFFFFFFL;
int32_t MAX_BUFFER_DOCS = 100000000;
int32_t MERGE_FACTOR = 100000000;
int32_t MAX_LEAF_COUNT = 1024;
float MAXMBSortInHeap = 512.0 * 8;
int32_t DIMS = 1;

template <FieldType type>
constexpr bool is_slice_type() {
    return type == OLAP_FIELD_TYPE_VARCHAR || type == OLAP_FIELD_TYPE_CHAR;
}

// TODO: use macro 
template <FieldType field_type>
constexpr bool is_numeric_type() {
    return field_type == FieldType::OLAP_FIELD_TYPE_INT ||
           field_type == FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT ||
           field_type == FieldType::OLAP_FIELD_TYPE_BIGINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_SMALLINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_UNSIGNED_TINYINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_UNSIGNED_SMALLINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_TINYINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_DOUBLE ||
           field_type == FieldType::OLAP_FIELD_TYPE_FLOAT ||
           field_type == FieldType::OLAP_FIELD_TYPE_DATE ||
           field_type == FieldType::OLAP_FIELD_TYPE_DATE_V2 ||
           field_type == FieldType::OLAP_FIELD_TYPE_DATETIME ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL_V2 ||
           field_type == FieldType::OLAP_FIELD_TYPE_LARGEINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL32 ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL64 ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL128 ||
           field_type == FieldType::OLAP_FIELD_TYPE_BOOL;
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

    ~InvertedIndexColumnWriterImpl() =default;

    Status init() override {
        try {
            if constexpr (is_slice_type<field_type>()) {
                return init_fulltext_index();
            } else if constexpr (is_numeric_type<field_type>()) {
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
        _dir.reset(StarrocksCompoundDirectory::getDirectory(_fs, index_path.c_str(), true));
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

        int field_config = int(lucene::document::Field::STORE_NO) |
                           int(lucene::document::Field::INDEX_NONORMS);
        if (_parser_type == InvertedIndexParserType::PARSER_NONE) {
            field_config |= int(lucene::document::Field::INDEX_UNTOKENIZED);
        } else {
            field_config |= int(lucene::document::Field::INDEX_TOKENIZED);
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
private:
    rowid_t _rid = 0;
    uint32_t _row_ids_seen_for_bkd = 0;
    uint64_t _reverted_index_size;
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
    std::unique_ptr<StarrocksCompoundDirectory> _dir;
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