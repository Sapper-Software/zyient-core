package ai.sapper.cdc.core.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

@Getter
@Setter
public class ModuleInstance {
    private String module;
    private String name;
    private String ip;
    private String startTime;
    private String instanceId;

    public ModuleInstance() {
        instanceId = UUID.randomUUID().toString();
    }

    public ModuleInstance withStartTime(long startTime) {
        Date date = new Date(startTime);
        DateFormat df = new SimpleDateFormat("yyyyMMdd:HH:mm");
        this.startTime = df.format(date);
        return this;
    }

    public ModuleInstance withIp(InetAddress address) {
        if (address != null) {
            ip = address.toString();
        }
        return this;
    }

    public String id() {
        return String.format("%s/%s", module, name);
    }

    @JsonIgnore
    public boolean isInstance(@NonNull ModuleInstance target) {
        return (instanceId.compareTo(target.instanceId) == 0);
    }

    @JsonIgnore
    public boolean isModule(@NonNull ModuleInstance target) {
        boolean ret = false;
        if (module.compareTo(target.module) == 0) {
            if (name.compareTo(target.name) == 0) {
                ret = true;
            }
        }
        return ret;
    }
}
