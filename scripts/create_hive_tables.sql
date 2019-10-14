drop table if exists ctakes_annotated_docs_dummy;
drop table if exists ctakes_annotated_docs;

CREATE TABLE ctakes_annotated_docs_dummy(fname STRING, part STRING, parsed BOOLEAN, text STRING, annotations STRING) PARTITIONED BY (loaded STRING) STORED AS SEQUENCEFILE;
CREATE TABLE ctakes_annotated_docs(fname STRING, part STRING, parsed BOOLEAN, text STRING, annotations STRING) PARTITIONED BY (loaded STRING) STORED AS SEQUENCEFILE;
