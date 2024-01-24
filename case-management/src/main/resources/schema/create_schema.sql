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
DROP TABLE IF EXISTS `cm_case_actions`;

CREATE TABLE `cm_case_actions`
(
    id          VARCHAR(24)   NOT NULL,
    action      VARCHAR(62)   NOT NULL,
    description VARCHAR(1024) NOT NULL,
    PRIMARY KEY (id)
) ENGINE = Aria;

DROP TABLE IF EXISTS `cm_case_codes`;

CREATE TABLE `cm_case_codes`
(
    id          VARCHAR(24)   NOT NULL,
    name        VARCHAR(62)   NOT NULL,
    description VARCHAR(1024) NOT NULL,
    PRIMARY KEY (id)
) ENGINE = Aria;

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
    reason_code            VARCHAR(24)  NOT NULL,
    PRIMARY KEY (case_id, comment_id)
) ENGINE = Aria;
CREATE INDEX index_comments_by ON `cm_case_comments` (commented_by, case_id);
CREATE INDEX index_comments_parent ON `cm_case_comments` (parent_comment_case_id, parent_comment_id, case_id, comment_id);

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
    case_id               VARCHAR(64)   NOT NULL,
    case_name             VARCHAR(256)  NOT NULL,
    case_state            VARCHAR(64)   NOT NULL,
    error                 TEXT,
    description           VARCHAR(2048) NOT NULL,
    assigned_to           VARCHAR(128),
    assigned_to_type      VARCHAR(8),
    assigned_timestamp    NUMERIC(18),
    closed_by             VARCHAR(128),
    closed_by_type        VARCHAR(8),
    closed_timestamp      NUMERIC(18),
    parent_case_id        VARCHAR(64),
    properties            MEDIUMTEXT,
    created_by            VARCHAR(128)  NOT NULL,
    created_by_type       VARCHAR(8)    NOT NULL,
    created_timestamp     NUMERIC(18)   NOT NULL,
    time_created          NUMERIC(18)   NOT NULL,
    time_updated          NUMERIC(18)   NOT NULL,
    external_reference_id VARCHAR(128),
    PRIMARY KEY (case_id)
) ENGINE = Aria;
CREATE INDEX index_cases_state ON `cm_cases` (case_state, case_id);
CREATE INDEX index_cases_assigned_to ON `cm_cases` (assigned_to, case_id);
CREATE INDEX index_cases_parent_case ON `cm_cases` (parent_case_id, case_id);
CREATE INDEX index_cases_reference ON `cm_cases` (external_reference_id, case_id)

DROP TABLE IF EXISTS `cm_case_history`;

CREATE TABLE `cm_case_history`
(
    case_id           VARCHAR(64) NOT NULL,
    sequence          NUMERIC(20) NOT NULL,
    action_id         VARCHAR(24) NOT NULL,
    code_id           VARCHAR(24) NOT NULL,
    comment           VARCHAR(2048),
    change_json       MEDIUMTEXT,
    created_by        VARCHAR(128),
    created_by_type   VARCHAR(8)  NOT NULL,
    created_timestamp NUMERIC(18) NOT NULL,
    time_created      NUMERIC(18) NOT NULL,
    time_updated      NUMERIC(18) NOT NULL,
    PRIMARY KEY (case_id, sequence)
) ENGINE = Aria;
CREATE INDEX index_case_history_by ON `cm_case_history` (created_by, case_id);