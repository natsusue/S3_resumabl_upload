#include "s3_resumable.h"
#include <sstream>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/UploadPartRequest.h>
#include <aws/s3-crt/model/CreateMultipartUploadRequest.h>
#include <aws/s3-crt/model/CompletedMultipartUpload.h>
#include <aws/s3-crt/model/CompleteMultipartUploadRequest.h>

#if defined(_WIN32)
#include <windows.h>
#include <io.h>
#define realpath(N,R) _fullpath((R),(N),_MAX_PATH)
#undef GetMessage // s3 GetMessage
#ifndef PATH_MAX
#define PATH_MAX MAX_PATH
#endif
#else
#include <unistd.h>
#endif

void s3_resumable::S3Client::open(const std::string end_point, const std::string region, const std::string access_id,
                                  const std::string access_secret, const std::string bucket, const std::string object_key)
{
    std::lock_guard<std::mutex> lock(_mutex);
    _end_point = end_point;
    _region = region;
    _access_id = access_id;
    _access_secret = access_secret;
    _bucket = bucket;
    _object_key = object_key;
}

void s3_resumable::S3Client::close()
{
    std::lock_guard<std::mutex> lock(_mutex);

    if (_upload_thread.joinable())
        _upload_thread.join();
    _last_error.clear();

    _end_point.clear();
    _access_id.clear();
    _access_secret.clear();
    _bucket.clear();
    _object_key.clear();

    _state = UploadState::kUploadWait;
}

const char * s3_resumable::S3Client::getLastError()
{
    std::lock_guard<std::mutex> lock(_mutex);
    return _last_error.c_str();

}

bool s3_resumable::S3Client::uploadFrom(const char * path)
{
    if (nullptr == path || 0 == strcmp(path, ""))
    {
        printf("Invaild local file path.\n");
        _last_error = "Invaild local file path.";
        return false;
    }
    if (_upload_thread.joinable())
    {
        printf("Upload is running! Can‘t create new upload request\n");
        return false;
    }

    bool succ = false;
    try
    {
        _upload_thread = std::thread(&s3_resumable::S3Client::uploadMultipartThread, this);
        succ = true;
        _state = UploadState::kUploadRunning;
    }
    catch (const std::system_error & e)
    {
        printf("std::thread create with system error, code is %d, msg is %s\n",
               e.code().value(), e.what());
    }
    catch (const std::exception & e)
    {
        printf("std::thread create with exception, msg is %s\n", e.what());
    }
    catch (...)
    {
        printf("std::thread create with exception\n");
    }

    if (!succ)
        _state = UploadState::kUploadFailed;
    return succ;
}

void s3_resumable::S3Client::uploadMultipartThread()
{
    // init client config
    Aws::S3Crt::ClientConfiguration client_config;
    constexpr double target_gbps = 5.0;
    constexpr uint64_t part_size = 52 * 1024 * 1024; // 52 MB.
    if (_region.empty())
        client_config.region = "defaultRegion";
    else
        client_config.region = _region;
    client_config.endpointOverride = _end_point;
    client_config.throughputTargetGbps = target_gbps;
    client_config.partSize = part_size;
    client_config.scheme = Aws::Http::Scheme::HTTP;

    // 
    const std::shared_ptr<Aws::Auth::AWSCredentialsProvider> cred =
        Aws::MakeShared<Aws::Auth::SimpleAWSCredentialsProvider>("s3client", _access_id, _access_secret);
    const Aws::S3Crt::S3CrtClient s3_crt_client(cred, client_config);


    // init multipart upload    
    Aws::S3Crt::Model::CreateMultipartUploadRequest request;
    request.SetBucket(_bucket);
    request.SetKey(_object_key);

    if (!_content_type.empty())
    {
        Aws::String content_type = static_cast<Aws::String>(_content_type);
        request.SetContentType(content_type);
    }
    auto multipart_outcome = s3_crt_client.CreateMultipartUpload(request);
    std::string upload_id = multipart_outcome.GetResult().GetUploadId();


    char cp_file[PATH_MAX] = { 0 };
    snprintf(cp_file, PATH_MAX, "%s.%s", _local_file_path.c_str(), "scp");

    size_t file_size = s3_resumable::utils::get_file_size(_local_file_path);
    int upload_succ_num = 1;
    int part_num = 1;
    int64_t offset = 0;
    int64_t tmp_offset = 0;
    s3_resum_info_t resum_info;
    bool need_upload = true;

    if (0 == access(cp_file, 0)) // 文件存在就解析
        parseCheckpointFile(cp_file, resum_info, upload_succ_num, tmp_offset);
    else    // 文件不存在时写入基础信息
    {
        FILE * fp = fopen(cp_file, "w");
        int64_t part_count = ((file_size + part_size - 1) / part_size);
        char * first_line_buffer = new char[4096];
        snprintf(first_line_buffer, 4096, "upload_id=%s:path=%s:size=%zu:bucket=%s:key=%s:part_count=%lld\n",
                 upload_id.c_str(), _local_file_path.c_str(), file_size, _bucket.c_str(), _object_key.c_str(), part_count);
        fwrite(first_line_buffer, 1, strlen(first_line_buffer), fp);
        fflush(fp);
        fclose(fp);
        delete[] first_line_buffer;
    }
    // 判断是否需要断点续传
    if (tmp_offset != 0 && _bucket == resum_info.bucket && _object_key == resum_info.key)
    {
        if (upload_succ_num == resum_info.part_count) //已经上传完了所有的分片
            need_upload = false;
        part_num = upload_succ_num + 1;
        offset = tmp_offset + part_size;
        upload_id = resum_info.upload_id;
    }
    _checkpoint_fp = std::fstream(cp_file, std::ios::in | std::ios::app | std::ios::ate);
    if (!_checkpoint_fp.good())
    {
        char * errorbuf = new char[4096];
        snprintf(errorbuf, 4096, "Open %s failed, errno:%d, str:%s.\n", cp_file, errno, strerror(errno));
        _last_error = errorbuf;
        delete[] errorbuf;
        fputs(_last_error.c_str(), stdout);
        fflush(stdout);
        _state = UploadState::kUploadFailed;
        return;
    }

    printf("multiplarts upload id is:%s.\n", upload_id.c_str());

    std::shared_ptr<Aws::IOStream> fileStream =
        Aws::MakeShared<Aws::FStream>("s3client", _local_file_path.c_str(),
                                      std::ios_base::in | std::ios_base::binary);
    if (!fileStream->good())
    {
        printf("Failed to open file: '%s'!\n", _local_file_path.c_str());
        _last_error = "Failed to open" + _local_file_path;
        _state = UploadState::kUploadFailed;
        return;
    }
    if (offset != 0)
        fileStream->seekg(offset, std::ios::beg);

    // upload part
    char * buffer = new char[part_size];
    memset(buffer, 0, part_size);
    std::streamsize current_filepos = 0;

    bool succ = true;
    while (!fileStream->eof() && need_upload)
    {
        fileStream->read(buffer, part_size);
        std::streamsize chars_read = fileStream->gcount();

        std::string buffer_std(buffer, chars_read);
        Aws::String buffer_aws(buffer_std.c_str(), buffer_std.size());

        std::string range = "bytes " + std::to_string(current_filepos) + "-" +
            std::to_string(current_filepos + chars_read - 1) + "/*";
        Aws::String range_aws(range.c_str(), range.size());

        const std::shared_ptr<Aws::IOStream> buffer_ptr =
            Aws::MakeShared<std::stringstream>("SampleAllocationTag", buffer_std);

        Aws::S3Crt::Model::UploadPartRequest upload_request;
        upload_request.SetBucket(_bucket);
        upload_request.SetKey(_object_key);
        upload_request.SetPartNumber(part_num);
        upload_request.SetUploadId(upload_id.c_str());
        upload_request.SetBody(buffer_ptr);

        auto upload_part_outcome = s3_crt_client.UploadPart(upload_request);

        // finall upload one part
        if (upload_part_outcome.IsSuccess())
        {
            std::string etag = upload_part_outcome.GetResult().GetETag();
            char checkpoint_line[1024];
            int line_len = snprintf(checkpoint_line, 1024, "etag=%s:offset=%lld:part_num=%d\n",
                                    etag.c_str(), offset, part_num);
            ++part_num;
            offset += part_size;
            _checkpoint_fp.write(checkpoint_line, line_len);
            _checkpoint_fp.flush();
        }
        else
        {
            succ = false;
            printf("Upload part %d failed.\n", part_num);
            _last_error = "Upload part ";
            _last_error.append(std::to_string(part_num)).append(" failed.");
            _state = UploadState::kUploadFailed;
            return;
        }
    }
    if (succ)
    {
        if (finishUploadMultipart(upload_id, s3_crt_client))
        {
            printf("File %s successfully uploaded to bucket %s object_key %s!\n", 
                   _local_file_path.c_str(), _bucket.c_str(), _object_key.c_str());
            _state = UploadState::kUploadSuccess;
        }
        else
        {
            printf("File %s upload all part success, but complate mulitpart upload request failed.\n", _local_file_path.c_str());
            _state = UploadState::kUploadFailed;
        }
    }
    else
    {
        printf("File %s upload all part failed.", _local_file_path.c_str());
        _state = UploadState::kUploadFailed;
    }
}

bool s3_resumable::S3Client::parse_kv_str(const char * line , std::unordered_map<std::string, std::string> & resault)
{
    std::stringstream params_ss;
    params_ss << line;
    for (std::string kv; std::getline(params_ss, kv, ','); )
    {
        if (kv.empty())
        {
            printf("Empty line\n");
            return false;
        }

        size_t pos = kv.find_first_of('=');
        std::string k;
        std::string v;
        if (std::string::npos == pos)
        {
            k = kv;
            v = "";
        }
        else
        {
            k = kv.substr(0, pos);
            v = kv.substr(pos + 1);
        }

        if (k.empty())
        {
            printf("Empty kv str.\n");
            continue;
        }

        size_t pos_first_not = k.find_first_not_of(" ");
        if (pos_first_not != std::string::npos)
            k.erase(0, pos_first_not);
        size_t pos_last_not = k.find_last_not_of(" ");
        if (pos_last_not != std::string::npos)
            k.erase(pos_last_not + 1);

        resault.emplace(k, v);
    }
    return true;
}

void s3_resumable::S3Client::parseCheckpointFile(const char * path, s3_resum_info_t & info, int & num, int64_t & offset)
{
    if (nullptr == path)
    {
        printf("Invaild checkpoint file path.\n");
        return;
    }

    if (0 != access(path, 0)) // F_OK
    {
        printf("%s not exist.\n", path);
        num = 1;
        offset = 0;
        return;
    }

    std::ifstream in(path);
    if (!in.good())
    {
        printf("Open %s failed, errno:%d, str:%s", path, errno, strerror(errno));
        return;
    }

    char * buffer = new char[4096];
    std::unordered_map<std::string, std::string> kv_res;
    in.getline(buffer, 8192);
    // 第一行保存了桶名等信息
    {
        if (!parse_kv_str(buffer, kv_res))
        {
            printf("parse fisrt line failed.\n");
            delete[] buffer;
            return;
        }
        std::string s_size = kv_res["size"];
        std::string s_part_count = kv_res["part_count"];
        if (s_size.empty())
            s_size = "0";
        if (s_part_count.empty())
            s_part_count = "0";

        std::string path = kv_res["path"];
        info.upload_id = kv_res["upload_id"];
        info.size = atoll(s_size.c_str());
        info.bucket = kv_res["bucket"];
        info.key = kv_res["key"];
        info.part_count =atoi(s_part_count.c_str());
    }
    if (in.eof())
    {   // 文件只有一行
        offset = 0;
        num = 1;
        in.close();
        delete[] buffer;
        return;
    }
    // 没有校验etag, 所以只需要读取最后一行记录的分片数和偏移量
    in.seekg(-2, std::ios_base::end);   // eof前
    bool keepLooping = true;
    while (keepLooping)
    {
        char ch;
        in.get(ch);
        if (ch == '\n')
            keepLooping = false;
        else
            in.seekg(-2, std::ios_base::cur);
    }

    kv_res.erase(kv_res.begin(), kv_res.end());
    kv_res.clear();

    in.getline(buffer, 4096);
    {
        if (!parse_kv_str(buffer, kv_res))
        {
            printf("parse fisrt line failed.\n");
            delete[] buffer;
            return;
        }

        std::string s_num = kv_res["part_num"];
        std::string s_offset = kv_res["offset"];

        if (s_num.empty() || s_offset.empty())
        {
            num = 0;
            offset = 0;
        }
        else
        {
            num = atoi(s_num.c_str());
            offset = atoll(s_offset.c_str());
        }
    }
    delete[] buffer;
    in.close();
}

bool s3_resumable::S3Client::finishUploadMultipart(const std::string & upload_id, const Aws::S3Crt::S3CrtClient & s3_crt_client)
{
    if (upload_id.empty())
    {
        printf("empty upload id.\n");
        return false;
    }
    Aws::S3Crt::Model::CompletedMultipartUpload complate_part;

    char * checkpoint_line = new char[4096];
    _checkpoint_fp.seekg(0, std::ios_base::beg);
    _checkpoint_fp.getline(checkpoint_line, 4096);
    // 读取每一个分片的信息
    while (_checkpoint_fp.getline(checkpoint_line, 4096))
    {
        std::unordered_map<std::string, std::string> kv_res;
        if (!parse_kv_str(checkpoint_line, kv_res))
        {
            printf("parse fisrt line failed.\n");
            delete[] checkpoint_line;
            return false;
        }
        std::string etag = kv_res["etag"];
        std::string s_part_num = kv_res["part_num"];
        if (s_part_num.empty())
        {
            printf("Get part num failed.\n");
            delete[] checkpoint_line;
            return false
                ;
        }
        int part_num = atoi(s_part_num.c_str());

        Aws::S3Crt::Model::CompletedPart sample_part;
        sample_part.SetETag(etag);
        sample_part.SetPartNumber(part_num);
        complate_part.AddParts(sample_part);
    }

    Aws::S3Crt::Model::CompleteMultipartUploadRequest complate_request;
    complate_request.SetBucket(_bucket);
    complate_request.SetKey(_object_key);
    complate_request.SetUploadId(upload_id);
    complate_request.WithMultipartUpload(complate_part);

    auto complate_outcome = s3_crt_client.CompleteMultipartUpload(complate_request);

    bool succ = true;
    if (!complate_outcome.IsSuccess())
    {
        auto msg = complate_outcome.GetError().GetMessage();
        auto code = complate_outcome.GetError().GetResponseCode();
        printf("Complate Multipart upload failed, err:%d, msg:%s\n", code, msg.c_str());
        succ = false;
    }
    _checkpoint_fp.close();
    char cp_file[PATH_MAX] = { 0 };
    snprintf(cp_file, PATH_MAX, "%s.%s", _local_file_path.c_str(), "scp");
    remove(cp_file);     // 无论是否成功拼接次分片都应该删除

    return succ;
}