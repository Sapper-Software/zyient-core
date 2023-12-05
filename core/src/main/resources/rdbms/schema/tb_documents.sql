DROP TABLE IF EXISTS `tb_documents`;

CREATE TABLE <db>.`tb_documents`
(
    doc_id        VARCHAR(64)   NOT NULL,
    collection    VARCHAR(64)   NOT NULL,
    parent_doc_id VARCHAR(64),
    doc_name      VARCHAR(1024) NOT NULL,
    doc_state     VARCHAR(32)   NOT NULL,
    mime_type     VARCHAR(256),
    URI           VARCHAR(2048) NOT NULL,
    created_by    VARCHAR(256)  NOT NULL,
    modified_by   VARCHAR(256)  NOT NULL,
    time_created  NUMERIC(11)   NOT NULL,
    time_updated  NUMERIC(11)   NOT NULL,
    properties    MEDIUMTEXT,
    --<reference_id> ...
    PRIMARY KEY (collection, doc_id),
    FULLTEXT(properties)
) ENGINE=Aria;

CREATE INDEX index_p_documents ON `tb_documents` (`parent_doc_id`);




