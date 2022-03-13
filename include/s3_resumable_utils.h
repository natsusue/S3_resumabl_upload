#pragma once
#include <string>

namespace s3_resumable
{
    namespace utils
    {
        // Get size of path
        size_t get_file_size(const char * path);
        size_t get_file_size(const std::string & path);
        
        //Get times
        int64_t get_time_ms();

        // get abs path
        std::string get_file_realpath(const char * path);
        std::string get_file_realpath(const std::string & path);
    }
}