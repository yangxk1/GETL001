package com.getl.converter;

import cn.hutool.core.collection.CollectionUtil;
import com.getl.constant.IRINamespace;
import com.getl.model.ug.IRI;
import com.getl.model.ug.UnifiedGraph;
import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * Mapping configuration between LPG and UG
 */
public class PropertiesGraphConfig {
    private boolean isNode;
    //Map<String, String>
    private final DualHashBidiMap labelMap = new DualHashBidiMap();
    /**
     * the edge label from this element
     */
    //Map<String,String>
    private final DualHashBidiMap iriToEdgeLabel = new DualHashBidiMap();
    private final List<String> edgeNamesapceList = new ArrayList<>();
    /**
     * the property label from this element
     */
    //Map<String, String>
    private final DualHashBidiMap iriToPropertiesName = new DualHashBidiMap();

    private IDTransform idTransform;


    public boolean isEdge(IRI label) {
        return iriToEdgeLabel.containsKey(label.getLocalName()) || withEdgeNamespace(label);
    }

    public String epLabel(IRI label) {
        if (iriToEdgeLabel.containsKey(label.getLocalName())) {
            return (String) iriToEdgeLabel.get(label.getLocalName());
        } else if (withEdgeNamespace(label)) {
            return label.getLocalName();
        }
        return (String) iriToPropertiesName.get(label.getLocalName());
    }

    public String getPop(IRI label) {
        return (String) this.iriToPropertiesName.get(label.getLocalName());
    }

    public PropertiesGraphConfig putPop(IRI label, String popName) {
        this.iriToPropertiesName.put(label.getLocalName(), popName);
        return this;
    }

    public String getEdge(IRI label) {
        if (iriToEdgeLabel.containsKey(label.getLocalName())) {
            return (String) this.iriToEdgeLabel.get(label.getLocalName());
        } else if (withEdgeNamespace(label)) {
            return label.getLocalName();
        }
        return label.getLocalName();
    }

    public String getEdge(String label) {
        BidiMap bidiMap = this.iriToEdgeLabel.inverseBidiMap();
        if (bidiMap.containsKey(label)) {
            return ((String) bidiMap.get(label)).substring(32);
        }
        return label;
    }

    public PropertiesGraphConfig putEdge(IRI label, String popName) {
        this.iriToEdgeLabel.put(label.getLocalName(), popName);
        return this;
    }

    public PropertiesGraphConfig setIdTransform(IDTransform idTransform) {
        this.idTransform = idTransform;
        return this;
    }

    public IDTransform getIdTransform() {
        if (idTransform == null) {
            idTransform = new IDTransform.DefaultIDTransform();
        }
        return idTransform;
    }

    public Set<String> epLabel(Set<IRI> labels) {
        Set<String> set = new HashSet<>();
        for (IRI label : labels) {
            // edge
            if (edgeNamesapceList.contains(label.getNameSpace())) {
                set.add(label.getLocalName());
            } else if (iriToEdgeLabel.containsKey(label.getLocalName())) {
                set.add((String) iriToEdgeLabel.get(label.getLocalName()));
            } else {
                set.clear();
                break;
            }
        }
        if (CollectionUtil.isNotEmpty(set)) {
            return set;
        }
        for (IRI label : labels) {
            //properties
            if (!iriToPropertiesName.containsKey(label.getLocalName())) {
                set.clear();
                break;
            }
            set.add((String) iriToPropertiesName.get(label.getLocalName()));
        }
        return set;
    }

    public String mapLabel(IRI label) {
        String LPGLabel = (String) this.labelMap.get(label.getLocalName());
        if (LPGLabel == null) {
            //TODO label转化的兜底方案
            LPGLabel = label.getLocalName();
        }
        return LPGLabel;
    }

    public IRI mapLabel(UnifiedGraph unifiedGraph, String label) {
        String IRILabel = (String) labelMap.inverseBidiMap().get(label);
        if (IRILabel == null) {
            //TODO label转化的兜底方案
            return unifiedGraph.getOrRegisterLabel(label);
        }
        return unifiedGraph.getOrRegisterLabel("", IRILabel);
    }

    public PropertiesGraphConfig putMapLabel(IRI label, String lpgLabel) {
        this.labelMap.put(label.getLocalName(), lpgLabel);
        return this;
    }

    public PropertiesGraphConfig putMapLabel(String label, String lpgLabel) {
        this.labelMap.put(label, lpgLabel);
        return this;
    }

    public PropertiesGraphConfig addEdgeNamespaceList(String nameSpace) {
        this.edgeNamesapceList.add(nameSpace);
        return this;
    }

    public IRI getPop(UnifiedGraph unifiedGraph, String name) {
        String popName = (String) this.iriToPropertiesName.inverseBidiMap().get(name);
        popName = popName == null ? name : popName;
        return unifiedGraph.getOrRegisterPopIRI(popName);
    }

    public interface IDTransform {
        public Object IRItoID(IRI iri);

        public IRI IDtoIRI(Object id, UnifiedGraph unifiedGraph);

        public static class DefaultIDTransform implements IDTransform {

            @Override
            public Object IRItoID(IRI iri) {
                return iri.getLocalName();
            }

            @Override
            public IRI IDtoIRI(Object id, UnifiedGraph unifiedGraph) {
                return unifiedGraph.getOrRegisterBaseIRI(IRINamespace.IRI_NAMESPACE, id.toString());
            }
        }
    }

    public static class PropertiesGraphConfigRegister {
        private Map<String, PropertiesGraphConfig> configs;

        public PropertiesGraphConfigRegister() {
            this.configs = new HashMap<>();
        }

        public PropertiesGraphConfig registerConfig(String... keys) {
            PropertiesGraphConfig propertiesGraphConfig = new PropertiesGraphConfig();
            for (String key : keys) {
                this.configs.put(key, propertiesGraphConfig);
            }
            return propertiesGraphConfig;
        }

        public PropertiesGraphConfig registerDefaultConfig() {
            PropertiesGraphConfig propertiesGraphConfig = new PropertiesGraphConfig();
            return propertiesGraphConfig;
        }

        public Map<String, PropertiesGraphConfig> getConfigs() {
            return this.configs;
        }
    }

    public boolean withEdgeNamespace(IRI popIRI) {
        if (edgeNamesapceList.contains(popIRI.getNameSpace())) {
            return true;
        }
        if (StringUtils.isNotBlank(popIRI.getNameSpace())) {
            return false;
        }
        for (String namespace : edgeNamesapceList) {
            if (popIRI.toString().startsWith(namespace)) {
                return true;
            }
        }
        return false;
    }
}
