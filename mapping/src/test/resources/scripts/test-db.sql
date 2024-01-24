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

DROP TABLE IF EXISTS tb_customers;

CREATE TABLE tb_customers
(
    id               VARCHAR(64)  NOT NULL,
    customer_name    VARCHAR(256) NOT NULL,
    email_id         VARCHAR(256),
    phone_no         VARCHAR(32)  NOT NULL,
    address          VARCHAR(512),
    address_city     VARCHAR(64),
    address_state    VARCHAR(64),
    address_country  VARCHAR(64),
    address_zip_code VARCHAR(32),
    country_code     VARCHAR(8),
    credit_limit     NUMERIC(18, 4),
    properties       TEXT,
    PRIMARY KEY (id)
);

DROP TABLE IF EXISTS tb_iso_country;

CREATE TABLE tb_iso_country
(
    name                     VARCHAR(128) NOT NULL,
    alpha_2                  VARCHAR(8)   NOT NULL,
    alpha_3                  VARCHAR(8)   NOT NULL,
    country_code             VARCHAR(8)   NOT NULL,
    iso_3166_2               VARCHAR(16)  NOT NULL,
    region                   VARCHAR(128),
    sub_region               VARCHAR(128),
    intermediate_region      VARCHAR(128),
    region_code              VARCHAR(16),
    sub_region_code          VARCHAR(16),
    intermediate_region_code VARCHAR(16),
    PRIMARY KEY (name)
);
