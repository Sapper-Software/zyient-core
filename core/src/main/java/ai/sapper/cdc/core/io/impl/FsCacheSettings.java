package ai.sapper.cdc.core.io.impl;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class FsCacheSettings extends Settings {
    public static final String __CONFIG_PATH = "cache";

    private static final int CACHE_SIZE = 128;
    private static final long CACHE_TIMEOUT = 5 * 60 * 1000;

    @Config(name = "size", required = false, type = Integer.class)
    private int cacheSize = CACHE_SIZE;
    @Config(name = "timeout", required = false, type = Long.class)
    private long cacheTimeout = CACHE_TIMEOUT;
}