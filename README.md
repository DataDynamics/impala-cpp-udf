# C++ UDF for Apache Impala

## Install

```
yum install boost-devel
```

## Build

```
clang++ -std=c++17 -O3 -emit-llvm -c RegexMaskingUdf.cc -o RegexMaskingUdf.bc -I /opt/cloudera/parcels/CDH/include

clang++ -std=c++17 -O3 -emit-llvm -c CachedRegexMaskingUdf.cc -o CachedRegexMaskingUdf.bc  -I /opt/cloudera/parcels/CDH/include -I /usr/include -licuuc -licuio -licui18n

```

## Registration

```
CREATE FUNCTION mask(STRING, STRING) RETURNS STRING
LOCATION 'hdfs:///user/impala/udf/RegexMaskingUdf.bc'
SYMBOL='mask';

CREATE FUNCTION mask(STRING, STRING) RETURNS STRING
LOCATION 'hdfs:///user/impala/udf/CachedRegexMaskingUdf.bc'
SYMBOL='mask';
```

## RegEx

`regex_rules.txt` 파일

```
# 키=정규표현식 (줄 단위)
APN=\\d{4}
EMAIL=[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}
SSN=\\d{6}-\\d{7}
```

## Execute

```
SELECT mask('APN', '내 번호는 010-1234-5678 입니다');
-- 결과: 내 번호는 010-****-**** 입니다
```


