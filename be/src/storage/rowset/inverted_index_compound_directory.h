// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/SharedHeader.h>
#include <CLucene/store/Directory.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/IndexOutput.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "fs/fs.h"
#include "io/input_stream.h"
#include "io/seekable_input_stream.h"

class CLuceneError;

namespace CL_NS(store) {
class LockFactory;
} // namespace lucene

namespace starrocks {

using FileSystemPtr = std::shared_ptr<FileSystem>;

class StarrocksCompoundFileWriter : LUCENE_BASE {
public:
    StarrocksCompoundFileWriter(CL_NS(store)::Directory* dir);
    ~StarrocksCompoundFileWriter() override =default;

    CL_NS(store)::Directory* getDirectory();
    void writeCompoundFile();
    void copyFile(const char* fileName,
                  CL_NS(store)::IndexOutput* output,
                  uint8_t* buffer, int64_t bufferLength);

private:
    CL_NS(store)::Directory* directory;
};

class CLUCENE_EXPORT StarrocksCompoundDirectory final: public CL_NS(store)::Directory {
public:

    class FSIndexOutput;
    class FSIndexInput;

    friend class StarrocksCompoundDirectory::FSIndexOutput;
    friend class StarrocksCompoundDirectory::FSIndexInput;

    ~StarrocksCompoundDirectory() override =default;

    FileSystemPtr getFileSystem() { return fs; }
    FileSystemPtr getCompoundFileSystem() { return compound_fs; }

    bool list(std::vector<std::string>* names) const override;
    bool fileExists(const char* name) const override;
    const char* getCfsDirName() const;
    const char* getObjectName() const override;

    static const char* getClassName();
    static StarrocksCompoundDirectory* getDirectory(FileSystemPtr fs, const char* file,
                                                    CL_NS(store)::LockFactory* lock_factory = nullptr,
                                                    FileSystemPtr cfs_fs = nullptr,
                                                    const char* cfs_file = nullptr);

    static StarrocksCompoundDirectory* getDirectory(FileSystemPtr fs, const char* file,
                                                    bool use_compound_file_writer,
                                                    FileSystemPtr  cfs_fs = nullptr,
                                                    const char* cfs_file = nullptr);

    int64_t fileModified(const char* name) const override;
    int64_t fileLength(const char* name) const override;
    bool deleteDirectory();
    bool openInput(const char* name, 
                   CL_NS(store)::IndexInput*& ret,
                   CLuceneError& err,
                   int32_t bufferSize = -1) override;

    void renameFile(const char* from, const char* to) override;
    void touchFile(const char* name) override;
    void close() override;
    CL_NS(store)::IndexOutput* createOutput(const char* name) override;
    std::string toString() const override;
protected:
    StarrocksCompoundDirectory();

    /// Removes an existing file in the directory.
    bool doDeleteFile(const char* name) override;

    void init(FileSystemPtr fs, const char* path,
              CL_NS(store)::LockFactory* lock_factory = nullptr,
              FileSystemPtr compound_fs = nullptr,
              const char* cfs_path = nullptr);

    void priv_getFN(char* buffer, const char* name) const;
private:
    void create();

    static bool disableLocks;

    bool useCompoundFileWriter {false};
    int filemode;
    std::mutex _this_lock;
    std::string directory;
    std::string cfs_directory;
    FileSystemPtr fs;
    FileSystemPtr compound_fs;
};

class StarrocksCompoundDirectory::FSIndexInput : public CL_NS(store)::BufferedIndexInput {
public:
    ~FSIndexInput() override;

    static const char* getClassName() { return "FSIndexInput"; }
    static bool open(FileSystemPtr fs, const char* path, IndexInput*& ret,
                     CLuceneError& error, int32_t bufferSize = -1);

    IndexInput* clone() const override;
    void close() override;
    int64_t length() const override { return _handle->_length; }

    const char* getDirectoryType() const override { return StarrocksCompoundDirectory::getClassName(); }
    const char* getObjectName() const override { return getClassName(); }

    std::mutex _this_lock;
protected:
    FSIndexInput(const FSIndexInput& clone);
    // Random-access methods
    void seekInternal(const int64_t position) override;
    // IndexInput methods
    void readInternal(uint8_t* b, const int32_t len) override;

private:
    struct SharedHandle : LUCENE_REFBASE {
        std::unique_ptr<io::SeekableInputStreamWrapper> _reader;
        uint64_t _length;
        int64_t _fpos;
        std::mutex* _shared_lock;
        char _path[4096];

        SharedHandle(const char* path);
        ~SharedHandle() override;
    };

    SharedHandle* _handle;
    int64_t _pos;

    FSIndexInput(SharedHandle* handle, int32_t buffer_size) : BufferedIndexInput(buffer_size) {
        this->_pos = 0;
        this->_handle = handle;
    }
};

} // namespace Starrocks
