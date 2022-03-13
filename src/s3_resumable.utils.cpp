#define _CRT_SECURE_NO_WARNINGS
#include "s3_resumable_utils.h"

#include <chrono>
#include <fstream>
#include <cstring>
#include <cstdlib>
#ifndef __linux__
#include <windows.h>
#define realpath(N,R) _fullpath((R),(N), _MAX_PATH)
#define PATH_MAX _MAX_PATH
constexpr char PATH_SEP = '\\';
#else
#include <unistd.h>
constexpr char PATH_SEP = '/';
#endif


size_t s3_resumable::utils::get_file_size(const char * path)
{
    if (nullptr == path || 0 == strcmp(path, ""))
    {
        printf("Invaild path.\n");
        return 0;
    }

    std::ifstream fp(path, std::ios::in | std::ios::binary);
    if (!fp.good())
    {
        printf("Failed to open file %s!\n", path);
        return 0;
    }

    if (!fp.seekg(0, std::ios::end))
    {
        fp.close();
        printf("Failed to seekg end of file %s!\n", path);
        return 0;
    }

    const std::streampos file_size = fp.tellg();
    fp.close();

    return file_size;
}

size_t s3_resumable::utils::get_file_size(const std::string & path)
{
    if (path.empty())
    {
        printf("Invaild path.\n");
        return 0;
    }
    return get_file_size(path.c_str());
}

int64_t s3_resumable::utils::get_time_ms()
{
    int64_t ts = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return ts * 1000;
}

std::string s3_resumable::utils::get_file_realpath(const char * path)
{
    if (nullptr == path || 0 == strcmp(path, ""))
    {
        printf("Invaild path.\n");
        return "";
    }

    char abs_path[PATH_MAX] = { 0 };
    if (realpath(path, abs_path) == nullptr)
    {
        int err = errno;
        std::string err_str = strerror(err);
        printf("Realpath %s failed with %d, %s!\n", path, err, err_str.c_str());
        return "";
    }
   
    return abs_path;
}

std::string s3_resumable::utils::get_file_realpath(const std::string & path)
{
    if (path.empty())
    {
        printf("Invaild path.\n");
        return "";
    }

    return get_file_realpath(path.c_str());
}