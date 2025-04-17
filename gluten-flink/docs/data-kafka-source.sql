CREATE TABLE srcTbl (
    d string
 ) WITH (
    'connector'='kafka',
    'topic' = 'test_in_1',
    'properties.bootstrap.servers' = 'sg-test-kafka-conn1.bigdata.bigo.inner:9093,sg-test-kafka-conn2.bigdata.bigo.inner:9093,sg-test-kafka-conn3.bigdata.bigo.inner:9093',
    'properties.group.id' = 'abcd123',
    'properties.auto.offset.reset' = 'latest',
    'format' = 'raw'
 );
CREATE TABLE snkTbl (d string) WITH ('connector'='print');

INSERT INTO snkTbl SELECT d FROM srcTbl;

CREATE TABLE src_json_Tbl (
    a int,
    b bigint,
    c smallint,
    d tinyint,
    f float,
    g double,
    h boolean,
    e Timestamp,
    r ROW<x int, y float, z string>,
    y ARRAY<string>,
    m MAP<string, string>
 ) WITH (
    'connector'='kafka',
    'topic' = 'test_in_1',
    'properties.bootstrap.servers' = 'sg-test-kafka-conn1.bigdata.bigo.inner:9093,sg-test-kafka-conn2.bigdata.bigo.inner:9093,sg-test-kafka-conn3.bigdata.bigo.inner:9093',
    'properties.group.id' = 'abcd123',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
 );

 create table snk_json_Tbl(
    a int,
    b bigint,
    c smallint,
    d tinyint,
    f float,
    g double,
    h boolean,
    e Timestamp,
    r ROW<x int, y float, z string>,
    y ARRAY<string>,
    m MAP<string, string>
    ) with('connector' = 'print');

 insert into snk_json_Tbl select a,b,c,d,f,g,h,e,r,y,m from src_json_Tbl;


{"a":123, "b": 1234545678, "c":123, "d": 5, "f": true, "g": 12.3348, "h": 9.22355, "i": "abcdedf"}
{"a":123, "b": 1234545678, "c":123, "d": 5}
{"a":123, "b": 1234545678, "c":123, "d": 5, "f": 12.3448, "g":100.234455955, "h":false, "e":1745735489798}
{"a":123, "b": 1234545678, "c":123, "d": 5, "f": 12.3448, "g":100.234455955, "h":false, "e":1745735489798, "r": {"x":111, "y":12.33, "z":"z1234"}, "y":["y123", "y124", "y125"]}
{"a":123, "b": 1234545678, "c":123, "d": 5, "f": 12.3448, "g":100.234455955, "h":false, "e":1745735489798, "r": {"x":111, "y":12.33, "z":"z1234"}, "y":["y123", "y124", "y125"], "m":{"cc":"c134", "dd":"d123", "ee":"e123"}
{"a":123, "b": 1234545678, "c":123, "d": 5, "f": 12.3448, "g":100.234455955, "h":false, "e":1745735489798, "r": {"x":111, "y":12.33, "z":"z1234"}, "y":["y123", "y124", "y125"], "m":{"cc":"c134", "dd":"d123", "ee":"e131"}}
{"a":123, "b": 1234545678, "c":123, "d": 5, "f": 12.3448, "g":100.234455955, "h":false, "e":"2025-05-06 11:30:00", "r": {"x":111, "y":12.33, "z":"z1234"}, "y":["y123", "y124", "y125"], "m":{"cc":"c134", "dd":"d123", "ee":"e131"}}
{"a":123, "b": 1234545678, "c":123, "d": 5, "f": 12.3448, "g":100.234455955, "h":false, "e":"2025-05-06 11:30:00.123", "r": {"x":111, "y":12.33, "z":"z1234"}, "y":["y123", "y124", "y125"], "m":{"cc":"c134", "dd":"d123", "ee":"e131"}}