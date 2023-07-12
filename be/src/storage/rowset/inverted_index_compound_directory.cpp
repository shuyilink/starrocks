#include "storage/rowset/inverted_index_compound_directory.h"

#include "common/status.h"
#include "fs/fs.h"
#include "fs/writable_file_wrapper.h"
#include "util/tdigest.h"

#include <bits/types/FILE.h>
#include <cstddef>
#include <string>
#include <string_view>

#ifdef _CL_HAVE_IO_H
#include <io.h>
#endif
#ifdef _CL_HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef _CL_HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef _CL_HAVE_DIRECT_H
#include <direct.h>
#endif
#include <CLucene/SharedHeader.h>
#include <CLucene/LuceneThreads.h>
#include <CLucene/clucene-config.h>
#include <CLucene/debug/error.h>
#include <CLucene/store/LockFactory.h>
#include <CLucene/debug/mem.h>
#include <CLucene/index/IndexReader.h>
#include <CLucene/index/IndexWriter.h>
#include <CLucene/store/LockFactory.h>
#include <CLucene/store/RAMDirectory.h>
#include <CLucene/util/Misc.h>

#include <assert.h>
#include <errno.h> // IWYU pragma: keep
#include <glog/logging.h>
#include <stdio.h>
#include <string.h>
#include <wchar.h>

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <utility>
#include <filesystem>

#define CL_MAX_PATH 4096
#define CL_MAX_DIR CL_MAX_PATH

#if defined(_WIN32) || defined(_WIN64)
#define PATH_DELIMITERA "\\"
#else
#define PATH_DELIMITERA "/"
#endif

namespace starrocks {

using namespace std::literals;

const char* WRITE_LOCK_FILE = "write.lock";
const char* COMPOUND_FILE_EXTENSION = ".idx";
const int64_t MAX_HEADER_DATA_SIZE = 1024 * 128; // 128k

bool CompoundDirectory::disableLocks = false;

StarrocksCompoundFileWriter::StarrocksCompoundFileWriter(CL_NS(store)::Directory* dir) {
    if (dir == nullptr) {
        _CLTHROWA(CL_ERR_NullPointer, "directory cannot be null");
    }

    directory = dir;
}

CL_NS(store)::Directory* StarrocksCompoundFileWriter::getDirectory() {
    return directory;
}

void StarrocksCompoundFileWriter::writeCompoundFile() {
    // list files in current dir
    std::vector<std::string> files;
    directory->list(&files);
    // remove write.lock file
    auto it = std::find(files.begin(), files.end(), WRITE_LOCK_FILE);
    if (it != files.end()) {
        files.erase(it);
    }

    auto compound_dir = dynamic_cast<CompoundDirectory*>(directory);
    // sort file list by file length
    std::vector<std::pair<std::string, int64_t>> sorted_files;
    sorted_files.reserve(files.size());
    for (auto&& file : files) {
        sorted_files.emplace_back(
            file, compound_dir->fileLength(file.c_str()));
    }
    std::sort(sorted_files.begin(), sorted_files.end(),
              [](const auto& a, const auto& b) { return a.second < b.second; });

    int32_t file_count = sorted_files.size();

    std::filesystem::path cfs_path(compound_dir->getCfsDirName());
    // 1. write file entries to ram directory to get header length
    std::string idx_name = cfs_path.stem().c_str() + ".idx"s;
    CL_NS(store)::RAMDirectory ram_dir;
    auto out_idx = ram_dir.createOutput(idx_name.c_str());
    if (out_idx == nullptr) {
        LOG(WARNING) << "Write compound file error: RAMDirectory output is nullptr.";
        return;
    }

    std::unique_ptr<CL_NS(store)::IndexOutput> ram_output(out_idx);
    ram_output->writeVInt(file_count);
    // write file entries in ram directory
    // number of files, which data are in header
    int header_file_count = 0;
    int64_t header_file_length = 0;
    int64_t buffer_length = 16384;
    uint8_t ram_buffer[buffer_length];
    for (const auto& [file_name, file_length]: sorted_files) {
        ram_output->writeString(file_name);
        ram_output->writeLong(0);            // data offset
        ram_output->writeLong(file_length);
        header_file_length += file_length;
        if (header_file_length <= MAX_HEADER_DATA_SIZE) {
            copyFile(file_name.c_str(), ram_output.get(), ram_buffer, buffer_length);
            header_file_count++;
        }
    }
    auto header_len = ram_output->getFilePointer();
    ram_output->close();
    ram_dir.deleteFile(idx_name.c_str());
    ram_dir.close();

    // 2.
    auto compound_fs = compound_dir->getCompoundFileSystem();
    auto out_dir = CompoundDirectory::getDirectory(compound_fs, cfs_path.parent_path().c_str(), false);

    auto out = out_dir->createOutput(idx_name.c_str());
    if (out == nullptr) {
        LOG(WARNING) << "Write compound file error: CompoundDirectory output is nullptr.";
        return;
    }
    std::unique_ptr<CL_NS(store)::IndexOutput> output(out);
    output->writeVInt(file_count);
    // write file entries
    int64_t data_offset = header_len;
    uint8_t header_buffer[buffer_length];
    for (int i = 0; i < sorted_files.size(); ++i) {
        const auto& [file_name, file_length] = sorted_files[i];
        output->writeString(file_name); // FileName
        // DataOffset
        if (i < header_file_count) {
            // file data write in header, so we set its offset to -1.
            output->writeLong(-1);
        } else {
            output->writeLong(data_offset);
        }
        output->writeLong(file_length); // FileLength
        if (i < header_file_count) {
            // append data
            copyFile(file_name.c_str(), output.get(), header_buffer, buffer_length);
        } else {
            data_offset += file_length;
        }
    }
    // write rest files' data
    uint8_t data_buffer[buffer_length];
    for (int i = header_file_count; i < sorted_files.size(); ++i) {
        auto& [file_name, _] = sorted_files[i];
        copyFile(file_name.c_str(), output.get(), data_buffer, buffer_length);
    }
    out_dir->close();
    // NOTE: need to decrease ref count, but not to delete here,
    // because index cache may get the same directory from DIRECTORIES
    _CLDECDELETE(out_dir)
    output->close();
}

void StarrocksCompoundFileWriter::copyFile(const char* fileName, CL_NS(store)::IndexOutput* output,
                                           uint8_t* buffer, int64_t bufferLength) {
    CL_NS(store)::IndexInput* tmp = nullptr;
    CLuceneError err;
    if (!directory->openInput(fileName, tmp, err)) {
        throw err;
    }

    std::unique_ptr<CL_NS(store)::IndexInput> input(tmp);
    int64_t start_ptr = output->getFilePointer();
    int64_t length = input->length();
    int64_t remainder = length;
    int64_t chunk = bufferLength;

    while (remainder > 0) {
        int64_t len = std::min(std::min(chunk, length), remainder);
        input->readBytes(buffer, len);
        output->writeBytes(buffer, len);
        remainder -= len;
    }
    if (remainder != 0) {
        TCHAR buf[CL_MAX_PATH + 100];
        swprintf(buf, CL_MAX_PATH + 100,
                 _T("Non-zero remainder length after copying")
                 _T(": %d (id: %s, length: %d, buffer size: %d)"),
                 (int)remainder, fileName, (int)length, (int)chunk);
        _CLTHROWT(CL_ERR_IO, buf);
    }

    int64_t end_ptr = output->getFilePointer();
    int64_t diff = end_ptr - start_ptr;
    if (diff != length) {
        TCHAR buf[100];
        swprintf(buf, 100,
                 _T("Difference in the output file offsets %d ")
                 _T("does not match the original file length %d"),
                 (int)diff, (int)length);
        _CLTHROWA(CL_ERR_IO, buf);
    }
    input->close();
}

class CompoundDirectory::FSIndexOutput : public CL_NS(store)::BufferedIndexOutput {
public:
    FSIndexOutput() = default;

    void init(FileSystem* fileSystem, const char* path);
    ~FSIndexOutput() override;
    void close() override;
    int64_t length() const override;
protected:
    void flushBuffer(const uint8_t* b, const int32_t size) override;
private:
    std::unique_ptr<WritableFile> writer;
};

bool CompoundDirectory::FSIndexInput::open(FileSystem* fs, const char* path,
                                                IndexInput*& ret, CLuceneError& error,
                                                int32_t buffer_size) {
    CND_PRECONDITION(path != nullptr, "path is NULL");

    if (buffer_size == -1) {
        buffer_size = CL_NS(store)::BufferedIndexOutput::BUFFER_SIZE;
    }
    SharedHandle* h = _CLNEW SharedHandle(path);
    auto status = fs->new_random_access_file(path);
    if (!status.ok()) {
        error.set(CL_ERR_IO, "open file error");
    } else { 
        h->_reader = std::move(status.value());
    }

    //Check if a valid handle was retrieved
    if (h->_reader) {
        //Store the file length
        h->_length = h->_reader->get_size().value(); // todo: get_size
        h->_fpos = 0;
        ret = _CLNEW FSIndexInput(h, buffer_size);
        return true;
    } else {
        int err = errno;
        if (err == ENOENT) {
            error.set(CL_ERR_IO, "File does not exist");
        } else if (err == EACCES) {
            error.set(CL_ERR_IO, "File Access denied");
        } else if (err == EMFILE) {
            error.set(CL_ERR_IO, "Too many open files");
        } else {
            error.set(CL_ERR_IO, "Could not open file");
        }
    }
    delete h->_shared_lock;
    _CLDECDELETE(h)
    return false;
}

CompoundDirectory::FSIndexInput::FSIndexInput(const FSIndexInput& other)
        : BufferedIndexInput(other) {
    if (other._handle == nullptr) {
        _CLTHROWA(CL_ERR_NullPointer, "other handle is null");
    }

    std::lock_guard<std::mutex> wlock(*other._handle->_shared_lock);
    _handle = _CL_POINTER(other._handle);
    _pos = other._handle->_fpos; //note where we are currently...
}

CompoundDirectory::FSIndexInput::SharedHandle::SharedHandle(const char* path) {
    _length = 0;
    _fpos = 0;
    _shared_lock = new std::mutex();
    ::strncpy(_path, path, strlen(path));
}

CompoundDirectory::FSIndexInput::SharedHandle::~SharedHandle() {
    // if (_reader) {
    //     if (_reader->close().ok()) {
    //         _reader = nullptr;
    //     }
    // }
}

CompoundDirectory::FSIndexInput::~FSIndexInput() {
    FSIndexInput::close();
}

CL_NS(store)::IndexInput* CompoundDirectory::FSIndexInput::clone() const {
    return _CLNEW CompoundDirectory::FSIndexInput(*this);
}

void CompoundDirectory::FSIndexInput::close() {
    BufferedIndexInput::close();
    if (_handle != nullptr) {
        std::mutex* lock = _handle->_shared_lock;
        bool ref = false;
        {
            std::lock_guard<std::mutex> wlock(*lock);
            //determine if we are about to delete the handle...
            ref = (_LUCENE_ATOMIC_INT_GET(_handle->__cl_refcount) > 1);
            //decdelete (deletes if refcount is down to 0
            _CLDECDELETE(_handle);
        }

        //if _handle is not ref by other FSIndexInput, try to release mutex lock, or it will be leaked.
        if (!ref) {
            delete lock;
            lock = nullptr;
        }
    }
}

void CompoundDirectory::FSIndexInput::seekInternal(const int64_t position) {
    CND_PRECONDITION(position >= 0 && position < _handle->_length, "Seeking out of range");
    _pos = position;
}

/** IndexInput methods */
void CompoundDirectory::FSIndexInput::readInternal(uint8_t* b, const int32_t len) {
    CND_PRECONDITION(_handle != nullptr, "shared file handle has closed");
    CND_PRECONDITION(_handle->_reader != nullptr, "file is not open");
    std::lock_guard<std::mutex> wlock(*_handle->_shared_lock);

    int64_t position = getFilePointer();
    if (_pos != position) {
        _pos = position;
    }

    if (_handle->_fpos != _pos) {
        _handle->_fpos = _pos;
    }

    // Slice result {b, (size_t)len};
    auto ret = _handle->_reader->read_at(_pos, b, len);
    if (!ret.ok()) {
        _CLTHROWA(CL_ERR_IO, "read past EOF");
    }
    size_t bytes_read = ret.value();
    bufferLength = len;
    if (bytes_read != len) {
        _CLTHROWA(CL_ERR_IO, "read error");
    }
    _pos += bufferLength;
    _handle->_fpos = _pos;
}

void CompoundDirectory::FSIndexOutput::init(FileSystem* fs, const char* path) {

    WritableFileOptions options{.sync_on_close = false, .mode = FileSystem::MUST_CREATE};
    auto status = fs->new_writable_file(options, path);
    if (!status.ok()) {
        auto err = "fail to create compound file: "s + path;
        LOG(WARNING) << err;
        _CLTHROWA(CL_ERR_IO, err.c_str());
    }
    writer = std::move(status.value());
}

CompoundDirectory::FSIndexOutput::~FSIndexOutput() {
    if (writer) {
        try {
            FSIndexOutput::close();
        } catch (const CLuceneError& err) {
            //ignore errors...
            LOG(WARNING) << "FSIndexOutput deconstruct error: " << err.what();
        }
    }
}

void CompoundDirectory::FSIndexOutput::flushBuffer(const uint8_t* b, const int32_t size) {
    if (writer != nullptr && b != nullptr && size > 0) {
        Slice data {b, (size_t)size};
        Status st = writer->append(data);
        if (!st.ok()) {
            LOG(WARNING) << "File IO Write error: " << st.to_string();
        }
    } else {
        LOG(WARNING) << "File writer is nullptr, ignore flush.";
    }
}

void CompoundDirectory::FSIndexOutput::close() {
    try {
        BufferedIndexOutput::close();
    } catch (const CLuceneError& err) {
        LOG(WARNING) << "BufferedIndexOutput close error: " << err.what();
        if (err.number() != CL_ERR_IO) {
            throw;
        }
    }
    if (writer) {
        // Status ret = writer->finalize();
        Status ret = writer->sync();
        if (ret.ok()) {
            LOG(WARNING) << "writer finalize error: " << ret.to_string();
            _CLTHROWA(CL_ERR_IO, ret.to_string().c_str());
        }
        ret = writer->close();
        if (ret.ok()) {
            LOG(WARNING) << "writer close error: " << ret.to_string();
            _CLTHROWA(CL_ERR_IO, ret.to_string().c_str());
        }
    } else {
        LOG(WARNING) << "File writer is nullptr, ignore finalize and close.";
    }
    writer.reset();
}

int64_t CompoundDirectory::FSIndexOutput::length() const {
    CND_PRECONDITION(writer != nullptr, "file is not open");
    return writer->size();
}

CompoundDirectory::CompoundDirectory() {
    filemode = 0644;
    this->lockFactory = nullptr;
}

void CompoundDirectory::init(FileSystem* _fs, const char* _path,
                                      CL_NS(store)::LockFactory* lock_factory,
                                      FileSystem* cfs, const char* cfs_path) {
    fs = std::move(_fs);
    directory = _path;

    if (cfs == nullptr) {
        compound_fs = fs;
    } else {
        compound_fs = std::move(cfs);
    }
    if (cfs_path != nullptr) {
        cfs_directory = cfs_path;
    } else {
        cfs_directory = _path;
    }
    bool doClearLockID = false;

    if (lock_factory == nullptr) {
        if (disableLocks) {
            // lock_factory = CL_NS(store)::NoLockFactory::getNoLockFactory();
        } else {
            lock_factory = _CLNEW CL_NS(store)::FSLockFactory(directory.c_str(), this->filemode);
            doClearLockID = true;
        }
    }

    // setLockFactory(lock_factory);

    if (doClearLockID) {
        lockFactory->setLockPrefix(nullptr);
    }

    // It's meaningless checking directory existence in S3.
    if (fs->type() == FileSystem::S3) {
        return;
    }
    if (Status status = fs->path_exists(directory); !status.ok()) {
        auto err = "File system error: " + status.to_string();
        LOG(WARNING) << err;
        _CLTHROWA_DEL(CL_ERR_IO, err.c_str());
    }
}

void CompoundDirectory::create() {
    std::lock_guard<std::mutex> wlock(_this_lock);

    // clear old files
    std::vector<std::string> files;
    lucene::util::Misc::listFiles(directory.c_str(), files, false);
    for (const auto& file: files) {
        if (CL_NS(index)::IndexReader::isLuceneFile(file.c_str())) {
            if (unlink((directory + PATH_DELIMITERA + file).c_str()) == -1) {
                _CLTHROWA(CL_ERR_IO, "Couldn't delete file ");
            }
        }
    }
    lockFactory->clearLock(CL_NS(index)::IndexWriter::WRITE_LOCK_NAME);
}

void CompoundDirectory::priv_getFN(char* buffer, const char* name) const {
    buffer[0] = 0;
    ::strncpy(buffer, directory.c_str(), directory.size());
    ::strncat(buffer, PATH_DELIMITERA, 1);
    ::strncat(buffer, name, ::strlen(name));
}

const char* CompoundDirectory::getClassName() {
    return "CompoundDirectory";
}
const char* CompoundDirectory::getObjectName() const {
    return getClassName();
}

bool CompoundDirectory::list(std::vector<std::string>* names) const {
    CND_PRECONDITION(!directory.empty(), "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, "");
    std::vector<FileStatus> file_infos;
    if (auto status = fs->list_path(fl, &file_infos); !status.ok()) {
        return false;
    }
    names->reserve(file_infos.size());
    for (auto&& info : file_infos) {
        names->emplace_back(std::move(info.name));
    }
    return true;
}

bool CompoundDirectory::fileExists(const char* name) const {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    return fs->path_exists(fl).ok();
}

const char* CompoundDirectory::getCfsDirName() const {
    return cfs_directory.c_str();
}

CompoundDirectory* CompoundDirectory::getDirectory(FileSystem* fs, const char* file,
                                                                     bool use_compound_file_writer,
                                                                     FileSystem* cfs_fs,
                                                                     const char* cfs_file) {
    CompoundDirectory* dir =
            getDirectory(fs, file, (CL_NS(store)::LockFactory*)nullptr, cfs_fs, cfs_file);
    dir->useCompoundFileWriter = use_compound_file_writer;
    return dir;
}

//static
CompoundDirectory* CompoundDirectory::getDirectory(FileSystem* _fs, const char* _file,
                                                                     CL_NS(store)::LockFactory* lock_factory,
                                                                     FileSystem* _cfs,
                                                                     const char* _cfs_file) {
    if (!_file || !_file[0]) {
        _CLTHROWA(CL_ERR_IO, "Invalid directory");
    }

    if (!_fs->path_exists(_file).ok()) {
        mkdir(_file, 0777);
    }

    auto dir = _CLNEW CompoundDirectory();
    dir->init(std::move(_fs), _file, lock_factory, std::move(_cfs), _cfs_file);
    return dir;
}

int64_t CompoundDirectory::fileModified(const char* name) const {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    struct stat buf;
    char buffer[CL_MAX_DIR];
    priv_getFN(buffer, name);
    if (::stat(buffer, &buf) == -1) {
        return 0;
    } else {
        return buf.st_mtime;
    }
}

void CompoundDirectory::touchFile(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char buffer[CL_MAX_DIR];
    ::snprintf(buffer, CL_MAX_DIR, "%s%s%s", directory.c_str(), PATH_DELIMITERA, name);

    if (!fs->new_random_access_file(buffer).ok()) {
        _CLTHROWA(CL_ERR_IO, "IO Error while touching file");
    }
}

int64_t CompoundDirectory::fileLength(const char* name) const {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char buffer[CL_MAX_DIR];
    priv_getFN(buffer, name);
    auto ret = fs->get_file_size(buffer);
    if (!ret.ok()) { 
        return -1;
    }
    return ret.value();
}

bool CompoundDirectory::openInput(const char* name, CL_NS(store)::IndexInput*& ret,
                                           CLuceneError& error, int32_t bufferSize) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    return FSIndexInput::open(fs, fl, ret, error, bufferSize);
}

void CompoundDirectory::close() {
    if (useCompoundFileWriter) {
        StarrocksCompoundFileWriter* cfsWriter = _CLNEW StarrocksCompoundFileWriter(this);
        // write compound file
        cfsWriter->writeCompoundFile();
        // delete index path, which contains separated inverted index files
        deleteDirectory();
        _CLDELETE(cfsWriter)
    }
}

bool CompoundDirectory::doDeleteFile(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    return fs->delete_file(fl).ok();
}

constexpr std::string_view delete_err_prefix = "couldn't delete: ";
constexpr std::string_view rename_err_prefix = "couldn't rename: ";
constexpr std::string_view overwrite_err_prefix = "couldn't overwrite: ";

bool CompoundDirectory::deleteDirectory() {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, "");
    Status status = fs->delete_dir(fl);
    if (!status.ok()) {
        std::string suffix = status.to_string();
        char* err = _CL_NEWARRAY(char, delete_err_prefix.size() + suffix.length() + 1);
        ::strncpy(err, delete_err_prefix.data(), delete_err_prefix.size());
        ::strncat(err, suffix.c_str(), suffix.length());
        _CLTHROWA_DEL(CL_ERR_IO, err);
    }
    return true;
}

void CompoundDirectory::renameFile(const char* from, const char* to) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    std::lock_guard<std::mutex> wlock(_this_lock);
    char old[CL_MAX_DIR];
    priv_getFN(old, from);

    char nu[CL_MAX_DIR];
    priv_getFN(nu, to);

    if (fs->path_exists(nu).ok()) {
        if (auto status = fs->delete_dir(nu); !status.ok()) {
            std::string suffix = status.to_string();
            char* err = _CL_NEWARRAY(char,
                 delete_err_prefix.size() + strlen(to) +  suffix.size() + 1);
            ::strncpy(err, delete_err_prefix.data(), delete_err_prefix.size());
            ::strncat(err, to, ::strlen(to));
            ::strncat(err, suffix.data(), suffix.size());
            _CLTHROWA_DEL(CL_ERR_IO, err);
        }
    }
    if (rename(old, nu) != 0) {
        char buffer[4 + rename_err_prefix.size() + ::strlen(from) + ::strlen(nu)];
        ::strncpy(buffer, rename_err_prefix.data(), rename_err_prefix.size());
        ::strncat(buffer, from, ::strlen(from));
        ::strncat(buffer, " to ", 3);
        ::strncat(buffer, nu, ::strlen(nu));
        _CLTHROWA(CL_ERR_IO, buffer);
    }
}

CL_NS(store)::IndexOutput* CompoundDirectory::createOutput(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    auto status = fs->path_exists(fl);
    if (status.is_io_error()) {
        LOG(WARNING) << "Starrocks compound directory create output error: " << status.to_string();
        return nullptr;
    }
    if (status.ok()) {
        if (!fs->delete_file(fl).ok()) {
            char* err = _CL_NEWARRAY(char,
                 overwrite_err_prefix.size() + ::strlen(name) + 1);
            ::strncpy(err, overwrite_err_prefix.data(), overwrite_err_prefix.size());
            ::strncat(err, name, ::strlen(name));
            _CLTHROWA(CL_ERR_IO, err);
        }
        assert(fs->path_exists(fl).is_not_found());
    }
    auto ret = _CLNEW FSIndexOutput();
    try {
        ret->init(fs, fl);
    } catch (const CLuceneError& err) {
        LOG(WARNING) << "FSIndexOutput init error: " << err.what();
        _CLTHROWA(CL_ERR_IO, "FSIndexOutput init error");
    }
    return ret;
}

std::string CompoundDirectory::toString() const {
    return std::string("CompoundDirectory@") + this->directory;
}

} // namespace Starrocks
