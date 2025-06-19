#include <string>
#include <unordered_map>
#include <regex>
#include <mutex>
#include <memory> // for std::unique_ptr
#include "impala_udf/udf.h"

using namespace impala_udf;

// 1. UDF의 상태를 관리할 구조체 정의
//    정규식 패턴과 컴파일된 정규식 캐시, 그리고 스레드 동기화를 위한 뮤텍스를 포함합니다.
struct MaskState {
    std::mutex mtx;
    std::unordered_map<std::string, std::string> patterns;
    std::unordered_map<std::string, std::unique_ptr<std::regex>> regex_cache;

    // 생성자: UDF가 사용할 정규식 패턴들을 미리 정의합니다.
    MaskState() {
        patterns["APN"] = R"(\d{4})";
        patterns["EMAIL"] = R"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})";
        patterns["SSN"] = R"(\d{6}-\d{7})";
    }
};

// 2. Prepare 함수 구현
//    UDF가 실행되기 전, 상태(State)를 초기화하고 FunctionContext에 등록합니다.
//    이 함수는 각 Impala 노드의 실행 단위(fragment)마다 한 번만 호출됩니다.
void MaskPrepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    // FRAGMENT_LOCAL 스코프에서만 상태를 초기화하여, 동일 프래그먼트 내의 모든 UDF 호출이
    // 동일한 상태를 공유하도록 합니다. THREAD_LOCAL도 있지만, 이 경우엔 FRAGMENT_LOCAL이 더 효율적입니다.
    if (scope != FunctionContext::FRAGMENT_LOCAL) return;

    // MaskState 객체를 힙(heap)에 생성합니다.
    MaskState* state = new MaskState();
    
    // 생성된 상태 객체의 포인터를 FunctionContext에 저장합니다.
    // 이렇게 저장된 포인터는 메인 UDF나 Close 함수에서 다시 꺼내 쓸 수 있습니다.
    context->SetFunctionState(scope, state);
}

// 3. Close 함수 구현
//    UDF 실행이 완료된 후, Prepare에서 할당한 상태를 안전하게 해제합니다.
//    이 함수도 프래그먼트마다 한 번만 호출됩니다.
void MaskClose(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) return;

    // context에 저장해 두었던 MaskState 포인터를 가져옵니다.
    void* state_ptr = context->GetFunctionState(scope);
    
    // 포인터가 유효하다면, 원래 타입으로 캐스팅하여 delete를 호출합니다.
    // 이를 통해 MaskState 객체와 그 안의 모든 리소스(unique_ptr 등)가 안전하게 해제됩니다.
    if (state_ptr != nullptr) {
        delete reinterpret_cast<MaskState*>(state_ptr);
    }
}

// 헬퍼 함수: StringVal을 생성합니다. (수정 없음)
StringVal MakeStringVal(FunctionContext* context, const std::string& s) {
    if (s.empty()) {
        uint8_t* empty_buf = context->Allocate(0);
        return StringVal(empty_buf, 0);
    }
    uint8_t* buffer = context->Allocate(s.size());
    if (buffer == nullptr) return StringVal::null();
    memcpy(buffer, s.data(), s.size());
    return StringVal(buffer, s.size());
}


// 4. 메인 UDF 로직 수정
//    이제 전역 변수 대신 FunctionContext에서 상태를 가져와 사용합니다.
StringVal mask(FunctionContext* context,
               const StringVal& key,
               const StringVal& input,
               const StringVal& mask_val) {
    if (key.is_null || input.is_null || mask_val.is_null) return StringVal::null();
    
    // FunctionContext에서 Prepare 함수가 만들어 둔 상태(State) 객체를 가져옵니다.
    MaskState* state = reinterpret_cast<MaskState*>(context->GetFunctionState(FunctionContext::FRAGMENT_LOCAL));
    if (state == nullptr) {
        // Prepare가 제대로 호출되지 않았거나 실패한 경우
        context->SetError("Masking UDF state not prepared.");
        return StringVal::null(); 
    }

    std::string key_str(reinterpret_cast<const char*>(key.ptr), key.len);
    
    const std::regex* pattern = nullptr;
    {
        // 스레드 안전하게 정규식 캐시를 조회하고, 없으면 컴파일 후 저장합니다.
        // 여러 스레드가 동시에 이 UDF를 호출하더라도 mtx가 캐시 접근을 보호합니다.
        std::lock_guard<std::mutex> lock(state->mtx);
        auto it = state->regex_cache.find(key_str);
        if (it != state->regex_cache.end()) {
            pattern = it->second.get();
        } else {
            auto pattern_it = state->patterns.find(key_str);
            if (pattern_it != state->patterns.end()) {
                try {
                    // C++14 이상에서는 std::make_unique 사용 권장
                    auto compiled = std::unique_ptr<std::regex>(new std::regex(pattern_it->second));
                    pattern = compiled.get();
                    state->regex_cache[key_str] = std::move(compiled);
                } catch (const std::regex_error& e) {
                    context->SetError(e.what());
                    return StringVal::null();
                }
            }
        }
    }

    if (pattern == nullptr) return StringVal::null();

    std::string input_str(reinterpret_cast<const char*>(input.ptr), input.len);
    std::string mask_str(reinterpret_cast<const char*>(mask_val.ptr), mask_val.len);

    if (mask_str.length() != 1) return StringVal::null();
    char mask_char = mask_str[0];

    // 기존의 문자열 치환 로직 (가장 효율적인 방식으로 수정)
    std::string result;
    result.reserve(input_str.length()); // 미리 메모리를 할당하여 성능 향상

    std::sregex_iterator it(input_str.begin(), input_str.end(), *pattern);
    std::sregex_iterator end;
    auto last_match = input_str.cbegin();

    for (; it != end; ++it) {
        result.append(it->prefix().first, it->prefix().second);
        result.append(it->length(), mask_char);
        last_match = it->suffix().first;
    }
    result.append(last_match, input_str.cend());

    return MakeStringVal(context, result);
}
