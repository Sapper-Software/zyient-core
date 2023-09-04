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

package io.zyient.intake.model;

import com.codekutter.r2db.driver.impl.S3StoreConfig;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

@Getter
@Setter
@Entity
@Table(name = "config_ds_filesystem_aws")
public class IntakeS3DataStoreConfig extends S3StoreConfig {
    @Column(name = "bucket_processing")
    @ConfigValue(required = true)
    private String processingBucket;
    @Column(name = "bucket_processed")
    @ConfigValue(required = true)
    private String processedBucket;
    @Column(name = "bucket_error")
    @ConfigValue(required = true)
    private String errorBucket;
    @ConfigValue(name = "filter")
    @Column(name = "root_path_filter")
    private String rootPathFilter;
    @Column(name = "channel")
    @ConfigAttribute
    private EIntakeChannel channel;
    @Column(name = "partition_num")
    @ConfigValue
    private short partition = 0;
    
    public IntakeS3DataStoreConfig() {
    	
    }
    
    public IntakeS3DataStoreConfig(@Nonnull IntakeS3DataStoreConfig source) {
    	this.channel = source.channel;
    	this.errorBucket = source.errorBucket;
    	this.partition = source.partition;
    	this.processedBucket = source.processedBucket;
    	this.processingBucket = source.processingBucket;
    	this.rootPathFilter = source.rootPathFilter;
    	setAuditContextProvider(source.getAuditContextProvider());
    	setAuditContextProviderClass(source.getAuditContextProviderClass());
    	setAuditContextProvider(source.getAuditContextProvider());
		setAuditContextProviderClass(source.getAuditContextProviderClass());
		setAudited(source.isAudited());
		setAuditLogger(source.getAuditLogger());
		setDataStoreClass(source.getDataStoreClass());
		setDataStoreClassString(source.getDataStoreClassString());
		setDescription(source.getDescription());
		setMaxResults(source.getMaxResults());
		setName(source.getName());
		setBucket(source.getBucket());
		setCacheExpiryWindow(source.getCacheExpiryWindow());
		setConnectionName(source.getConnectionName());
		setConnectionType(source.getConnectionType());
		setConnectionTypeString(source.getConnectionTypeString());
		setDescription(source.getDescription());
		setMaxCacheSize(source.getMaxCacheSize());
		setTempDirectory(source.getTempDirectory());
		setUseCache(source.isUseCache());
    }
}
