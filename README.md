# C++ UDF for Apache Impala

## Install

```
yum install boost-devel
```

## Build

```
clang++ -std=c++17 -O3 -emit-llvm -c RegexMaskingUdf.cc -o RegexMaskingUdf.bc -I /opt/cloudera/parcels/CDH/include
```

## Registration

```
CREATE FUNCTION mask(STRING, STRING) RETURNS STRING
LOCATION 'hdfs:///user/impala/udf/RegexMaskingUdf.bc'
SYMBOL='mask';
```

## Execute

```
SELECT mask('APN', '내 번호는 010-1234-5678 입니다');
-- 결과: 내 번호는 010-****-**** 입니다
```


