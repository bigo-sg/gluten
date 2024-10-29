
#include "config.h"

#if USE_BZIP2
#include <IO/BoundedReadBuffer.h>
#include <IO/ReadSettings.h>
#include <IO/SplittableBzip2ReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Storages/ObjectStorage/HDFS/ReadBufferFromHDFS.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/Config/ConfigProcessor.h>

using namespace DB;

int main()
{
    setenv("LIBHDFS3_CONF", "/path/to/hdfs/config", true); /// NOLINT
    String hdfs_uri = "hdfs://cluster";
    String hdfs_file_path = "/path/to/bzip2/file";
    ConfigurationPtr config = Poco::AutoPtr(new Poco::Util::MapConfiguration());
    ReadSettings read_settings;
    std::unique_ptr<SeekableReadBuffer> in = std::make_unique<ReadBufferFromHDFS>(hdfs_uri, hdfs_file_path, *config, read_settings, 0, true);

    std::unique_ptr<SeekableReadBuffer> bounded_in = std::make_unique<BoundedReadBuffer>(std::move(in));
    size_t start = 805306368;
    size_t end = 1073900813;
    bounded_in->seek(start, SEEK_SET);
    bounded_in->setReadUntilPosition(end);

    std::unique_ptr<ReadBuffer> decompressed = std::make_unique<SplittableBzip2ReadBuffer>(std::move(bounded_in), true, true);

    String download_path = "./download.txt";
    WriteBufferFromFile write_buffer(download_path);
    copyData(*decompressed, write_buffer);
    return 0;
}
#endif
