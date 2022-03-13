#include "s3_resumable_utils.h"

#include <fstream>
#include <string>
#include <thread>
#include <fstream>
#include <atomic>
#include <mutex>
#include <map>

#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>

#include "s3_resumable_utils.h"

namespace s3_resumable
{
    enum class UploadState
    {
        kUploadWait = 0,    // wait open
        kUploadRunning,     // Uploading
        kUploadSuccess,     // upload success
        kUploadFailed,      // upload failed
    };

    typedef struct _s3_resume_into_t
    {
        std::string path = "";      // Local file path
        std::string bucket = "";    // Bucket 
        std::string key = "";       // Object key
        std::string upload_id = ""; 
        int64_t size = 0;           // Local file size
        int part_count = 0;         // Need upload part count
    }s3_resum_info_t;

 
    class S3Client
    {
    public:
        S3Client() = default;

        void open(const std::string end_point, const std::string region, const std::string _access_id, const std::string _access_secret,
                  const std::string _bucket, const std::string object_key);
        void close();
        bool uploadFrom(const char * local_file_path);

        const char * getLastError();

    protected:
        void uploadMultipartThread();
        void parseCheckpointFile(const char * path, s3_resum_info_t & info, int & num, int64_t & offset);
        bool finishUploadMultipart(const std::string & upload_id, const Aws::S3Crt::S3CrtClient & s3_crt_client);

        bool parse_kv_str(const char * line, std::unordered_map<std::string, std::string> & resault);

    private:
        // EP
        std::string _end_point;     
        // Region
        std::string _region;
        // Access Id
        std::string _access_id;
        // Access Secret
        std::string _access_secret;
        // Bucket
        std::string _bucket;
        // Object
        std::string _object_key;
        std::string _content_type;

        // 本地存储路径
        std::string _local_file_path;
        // Thread
        std::thread _upload_thread;

        // 断点续传文件句柄
        std::fstream _checkpoint_fp;

        std::atomic<UploadState> _state = UploadState::kUploadWait;
        //最近一次错误信息
        std::string _last_error;

        // Mutex
        std::mutex _mutex;
    };

}
