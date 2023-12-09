/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
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

DROP TABLE IF EXISTS `cm_case_comments`;

CREATE TABLE `cm_case_comments`
(
    case_id                VARCHAR(64)  NOT NULL,
    comment_id             NUMERIC(14)  NOT NULL,
    commented_by           VARCHAR(128) NOT NULL,
    commented_by_type      VARCHAR(8)   NOT NULL,
    comment_timestamp      NUMERIC(18)  NOT NULL,
    comment_state          VARCHAR(32)  NOT NULL,
    parent_comment_case_id VARCHAR(64),
    parent_comment_id      NUMERIC(14),
    artefact_collection    VARCHAR(64),
    artefact_id            VARCHAR(64),
    comment                MEDIUMTEXT,
    PRIMARY KEY (case_id, comment_id)
) ENGINE = Aria;

DROP TABLE IF EXISTS `cm_artefact_reference`;

CREATE TABLE `cm_artefact_reference`
(
    case_id       VARCHAR(64)  NOT NULL,
    collection    VARCHAR(64)  NOT NULL,
    doc_id        VARCHAR(64)  NOT NULL,
    artefact_type VARCHAR(256) NOT NULL,
    PRIMARY KEY (case_id, collection, doc_id)
) ENGINE = Aria;

DROP TABLE IF EXISTS `cm_cases`;

CREATE TABLE `cm_cases`
(
    case_id            VARCHAR(64)   NOT NULL,
    case_state         VARCHAR(64)   NOT NULL,
    error              TEXT,
    description        VARCHAR(2048) NOT NULL,
    assigned_to        VARCHAR(128),
    assigned_to_type   VARCHAR(8),
    assigned_timestamp NUMERIC(18),
    closed_by          VARCHAR(128),
    closed_by_type     VARCHAR(8),
    closed_timestamp   NUMERIC(18),
    parent_case_id     VARCHAR(64),
    properties         MEDIUMTEXT,
    created_by         VARCHAR(128),
    created_by_type    VARCHAR(8),
    time_created       NUMERIC(18),
    time_updated       NUMERIC(18),
    PRIMARY KEY (case_id)
) ENGINE = Aria;
