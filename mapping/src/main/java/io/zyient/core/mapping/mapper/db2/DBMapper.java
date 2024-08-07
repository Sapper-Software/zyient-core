package io.zyient.core.mapping.mapper.db2;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.*;

@Getter
@Setter
public class DBMapper {

    @Getter
    @Setter
    public static class DBConfNode {
        private DBMappingConf conf;
        private List<DBConfNode> nodes;

    }

    private List<DBConfNode> nodes;
    private List<DBMappingConf> confs;

    public DBMapper(List<DBMappingConf> confs) {
        this.confs = confs;
        nodes = new ArrayList<>();
        // get parents
        List<DBMappingConf> parents = new ArrayList<>();
        for (DBMappingConf mappingConf : confs) {
            if (StringUtils.isBlank(mappingConf.getParentId())) {
                parents.add(mappingConf);
            }
        }

        parents.forEach(c -> {
            DBConfNode dbConfNode = new DBConfNode();
            dbConfNode.conf = c;
            dbConfNode.nodes = this.getChildNodes(c);
            nodes.add(dbConfNode);
        });
    }


    private List<DBConfNode> getChildNodes(DBMappingConf conf) {
        List<DBConfNode> result = new ArrayList<>();

        List<DBMappingConf> children = confs.stream().filter(c -> c.getParentId() != null && c.getParentId().equals(conf.getKey().getKey())).toList();
        if (!CollectionUtils.isEmpty(children)) {
            children.forEach(c -> {
                DBConfNode dbConfNode = new DBConfNode();
                dbConfNode.conf = c;
                dbConfNode.nodes = getChildNodes(c);
                result.add(dbConfNode);
            });
        }
        if (result.isEmpty()) {
            return null;
        }
        return result;
    }

}
