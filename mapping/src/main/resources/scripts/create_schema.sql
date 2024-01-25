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

DROP TABLE IF EXISTS m_mapping_defs;

CREATE TABLE m_mapping_defs
(
    condition_id VARCHAR(64)  NOT NULL,
    sequence     NUMERIC(11)  NOT NULL,
    source_path  VARCHAR(256) NOT NULL,
    target_path  VARCHAR(256) NOT NULL,
    mapping_type VARCHAR(32)  NOT NULL,
    nullable     BOOL         NOT NULL default true,
    data_type    VARCHAR(64)  NOT NULL,
    PRIMARY KEY (condition_id, sequence)
);

DROP TABLE IF EXISTS m_regex_defs;

CREATE TABLE m_regex_defs
(
    condition_id        VARCHAR(64)  NOT NULL,
    sequence            NUMERIC(11)  NOT NULL,
    source_path         VARCHAR(256) NOT NULL,
    target_path         VARCHAR(256) NOT NULL,
    mapping_type        VARCHAR(32)  NOT NULL,
    nullable            BOOL         NOT NULL default true,
    data_type           VARCHAR(64)  NOT NULL,
    name                VARCHAR(64)  NOT NULL,
    regex               VARCHAR(512) NOT NULL,
    replace_with        VARCHAR(512),
    regex_groups        VARCHAR(256),
    replace_with_format VARCHAR(512),
    PRIMARY KEY (condition_id, sequence)
);

DROP TABLE IF EXISTS m_condition_defs;

CREATE TABLE m_condition_defs
(
    id               VARCHAR(64)  NOT NULL,
    name             VARCHAR(128) NOT NULL, -- [Column set to be changed to required query fields.]
    parent_id        VARCHAR(64),
    source_field     VARCHAR(256),
    condition_string VARCHAR(512) NOT NULL,
    data_type        VARCHAR(64)  NOT NULL,
    condition_type   VARCHAR(64)  NOT NULL default 'Simple',
    PRIMARY KEY (id)
);
CREATE INDEX index_condition_name ON m_condition_defs (name, id);