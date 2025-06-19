#include <string>
#include <unordered_map>
#include <regex>
#include <mutex>
#include "impala_udf/udf.h"

using namespace impala_udf;

class RegexCache {
public:
    static const std::regex* GetRegex(const std::string& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = regex_map_.find(key);
        if (it != regex_map_.end()) return it->second.get();

        auto regex_str_it = regex_patterns_.find(key);
        if (regex_str_it == regex_patterns_.end()) return nullptr;

        try {
            std::unique_ptr<std::regex> compiled(new std::regex(regex_str_it->second));
            const std::regex* ptr = compiled.get();
            regex_map_[key] = std::move(compiled);
            return ptr;
        } catch (const std::regex_error&) {
            return nullptr;
        }
    }

private:
    static inline std::unordered_map<std::string, std::string> regex_patterns_ = {
        {"APN", R"(\d{4})"},
        {"EMAIL", R"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"},
        {"SSN", R"(\d{6}-\d{7})"}
    };

    static inline std::unordered_map<std::string, std::unique_ptr<std::regex>> regex_map_;
    static inline std::mutex mutex_;
};

// main UDF
StringVal mask(FunctionContext* context,
               const StringVal& key,
               const StringVal& input,
               const StringVal& mask_val) {
    if (key.is_null || input.is_null || mask_val.is_null) return StringVal::null();

    std::string key_str(reinterpret_cast<const char*>(key.ptr), key.len);
    std::string input_str(reinterpret_cast<const char*>(input.ptr), input.len);
    std::string mask_str(reinterpret_cast<const char*>(mask_val.ptr), mask_val.len);

    if (mask_str.length() != 1) return StringVal::null();  // 단일 문자만 허용

    char mask_char = mask_str[0];

    const std::regex* pattern = RegexCache::GetRegex(key_str);
    if (!pattern) return StringVal::null(); // 정규표현식 없음

    std::string result = input_str;
    std::sregex_iterator it(input_str.begin(), input_str.end(), *pattern);
    std::sregex_iterator end;

    for (; it != end; ++it) {
        size_t start = it->position();
        size_t len = it->length();
        for (size_t i = 0; i < len; ++i) {
            result[start + i] = mask_char;
        }
    }

    StringVal out(context->Allocate(result.size()));
    if (!out.ptr) return StringVal::null();

    memcpy(out.ptr, result.data(), result.size());
    out.len = result.size();
    return out;
}
