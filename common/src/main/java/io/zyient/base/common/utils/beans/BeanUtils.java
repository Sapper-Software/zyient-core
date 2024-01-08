package io.zyient.base.common.utils.beans;

import io.zyient.base.common.cache.LRUCache;
import lombok.NonNull;

import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

public class BeanUtils {
    private static final ReentrantLock __cacheLock = new ReentrantLock();
    private static final LRUCache<Class<?>, ClassDef> __classDefs = new LRUCache<>(1024);

    public static ClassDef get(@NonNull Class<?> clazz) throws Exception {
        __cacheLock.lock();
        try {
            if (__classDefs.containsKey(clazz)) {
                Optional<ClassDef> o = __classDefs.get(clazz);
                if (o.isPresent()) return o.get();
            }
            ClassDef def = new ClassDef();
            def.name(clazz.getSimpleName());
            def.type(clazz);
            __classDefs.put(def.type(), def);
            def.from(clazz);
            return def;
        } finally {
            __cacheLock.unlock();
        }
    }

}
