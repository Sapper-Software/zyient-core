package ai.sapper.cdc.entity.utils;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.model.DirectoryInode;
import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

@Getter
@Accessors(fluent = true)
public abstract class JournalWriter<T> implements Closeable {
    public static class Constants {
        public static final String __PATH_FORMAT_HOUR = "yyyy/MM/dd/HH";
        public static final String __PATH_FORMAT_DAY = "yyyy/MM/dd";
        public static final String __PATH_FORMAT_MONTH = "yyyy/MM";
    }

    public enum EPathFormat {
        Month, Day, Hour, Minutes30, Minutes15;

        public static String path(@NonNull EPathFormat format) {
            return switch (format) {
                case Day -> DateTimeHelper.dateString(Constants.__PATH_FORMAT_DAY);
                case Month -> DateTimeHelper.dateString(Constants.__PATH_FORMAT_MONTH);
                case Hour -> DateTimeHelper.dateString(Constants.__PATH_FORMAT_HOUR);
                case Minutes30 -> getMinuteOffset(30);
                case Minutes15 -> getMinuteOffset(15);
            };
        }

        private static String getMinuteOffset(int b) {
            String dt = DateTimeHelper.dateString(Constants.__PATH_FORMAT_HOUR);
            DateTime d = new DateTime(System.currentTimeMillis());
            int m = d.minuteOfHour().get();
            int o = m / b;
            m *= o;
            return String.format("%s/%d", dt, m);
        }
    }

    private final BaseEnv<?> env;
    private final FileSystem fs;
    private final SchemaEntity entity;
    private final EPathFormat format;
    private final DirectoryInode root;
    private FileInode node;
    private Writer writer;

    protected JournalWriter(@NonNull BaseEnv<?> env,
                            @NonNull FileSystem fs,
                            @NonNull DirectoryInode root,
                            @NonNull SchemaEntity entity,
                            @NonNull EPathFormat format) {
        this.env = env;
        this.fs = fs;
        this.root = root;
        this.entity = entity;
        this.format = format;
    }

    public JournalWriter<T> open() throws IOException {
        String path = EPathFormat.path(format);
        node = fs.create(root, path);
        writer = fs.writer(node);
        return this;
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.commit(true);
            writer.close();
            writer = null;
        }
    }

    public abstract void write(@NonNull T record) throws IOException;

    public abstract void write(@NonNull List<T> records) throws IOException;
}
