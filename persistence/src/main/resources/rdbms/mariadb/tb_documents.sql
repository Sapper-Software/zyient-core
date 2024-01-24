/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

DROP TABLE IF EXISTS `tb_documents`;

CREATE TABLE `tb_documents`
(
    doc_id          VARCHAR(64)   NOT NULL,
    collection      VARCHAR(64)   NOT NULL,
    parent_doc_id   VARCHAR(64),
    doc_name        VARCHAR(256)  NOT NULL,
    doc_source_path VARCHAR(1024) NOT NULL,
    doc_state       VARCHAR(32)   NOT NULL,
    mime_type       VARCHAR(256),
    URI             VARCHAR(2048) NOT NULL,
    document_count  NUMERIC(8)    NOT NULL DEFAULT 0,
    created_by      VARCHAR(256)  NOT NULL,
    modified_by     VARCHAR(256)  NOT NULL,
    time_created    NUMERIC(18)   NOT NULL,
    time_updated    NUMERIC(18)   NOT NULL,
    passkey         VARCHAR(256),
    error           TEXT,
    properties      MEDIUMTEXT,
    reference_id    VARCHAR(256)  NOT NULL,
    PRIMARY KEY (collection, doc_id),
    FULLTEXT (properties)
) ENGINE = Aria;

CREATE INDEX index_p_documents ON `tb_documents` (`parent_doc_id`, `doc_id`);
CREATE INDEX index_source_name ON `tb_documents` (`doc_name`, `doc_id`);





