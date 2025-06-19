# C++ UDF for Apache Impala

## Install

```
yum install boost-devel
```

## Build

```
g++ -shared -fPIC -o libregexmask.so RegexMaskingUdf.cc -I /opt/cloudera/parcels/CDH/include
```

## Registration

```
# nm libregexmask.so | grep mask
0000000000075c2f T _Z4maskPN10impala_udf15FunctionContextERKNS_9StringValES4_S4_
0000000000098790 W _ZNSt12_Base_bitsetILm4EE10_S_maskbitEm
```

```
CREATE FUNCTION mask(STRING, STRING) RETURNS STRING
LOCATION 'hdfs:///user/impala/udf/RegexMaskingUdf.bc'
SYMBOL='_Z4maskPN10impala_udf15FunctionContextERKNS_9StringValES4_S4_';

CREATE FUNCTION mask(STRING, STRING) RETURNS STRING
LOCATION 'hdfs:///user/impala/udf/CachedRegexMaskingUdf.bc'
SYMBOL='_Z4maskPN10impala_udf15FunctionContextERKNS_9StringValES4_S4_';
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

```sql
SELECT mask('APN', '내 번호는 010-1234-5678 입니다');
-- 결과: 내 번호는 010-****-**** 입니다
```

## 기타

```sql
SHUW FUNCTIONS IN mydb;
SHOW CREATE FUNCTION mydb.mask;
```

