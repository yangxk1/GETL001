# RMSGConverter - RM与OneGraph相互转换器

## 概述

`RMSGConverter` 是一个用于在关系模型（Relational Model, RM）和 OneGraph（SG）之间进行相互转换的类，类似于 `RMMGConverter`（RM与MG的转换器）。

## 文件位置

```
/Users/yangxk/code/GETL001/src/main/java/com/getl/converter/SG/RMSGConverter.java
```

## 设计思路

### 1. 基于 RMMGConverter 的设计模式

参照 `RMMGConverter.java` 的实现，`RMSGConverter` 提供了双向转换：
- **RM → SG**: 将关系数据转换为 OneGraph 数据集
- **SG → RM**: 将 OneGraph 数据集转换回关系数据

### 2. 核心概念映射

#### RM → SG 转换

| RM 元素 | SG 元素 |
|---------|---------|
| 节点行 (Node Line) | OGSimpleNodeIRI |
| 节点列值 | OGPropertyStatement |
| 节点标签 (表名) | rdf:type 类型声明 |
| 边行 (Edge Line) | OGRelationshipStatement |
| 边属性 | 边的 OGPropertyStatement |
| 行 ID | IRI 本地名称 |

#### SG → RM 转换

| SG 元素 | RM 元素 |
|---------|---------|
| OGSimpleNodeIRI | 节点行 (Node Line) |
| OGPropertyStatement (节点) | 节点列值 |
| rdf:type 类型声明 | 节点标签 (表名) |
| OGRelationshipStatement | 边行 (Edge Line) |
| OGPropertyStatement (边) | 边属性 |
| IRI 本地名称 | 行 ID |

## 主要功能

### 1. RM → SG 转换

```java
public void addRMToSG()
```

**功能**：
- 遍历 RMGraph 中的所有 Lines
- 节点行转换为 OGSimpleNodeIRI 和相关的属性语句
- 边行转换为 OGRelationshipStatement
- 保留边的属性作为边的属性语句

**处理流程**：
1. 识别节点行（无 schema 或 schema.isNode() 为 true）
2. 创建 OGSimpleNodeIRI 节点
3. 添加列值作为 OGPropertyStatement
4. 添加表名作为 rdf:type 类型声明
5. 处理边行，创建 OGRelationshipStatement
6. 添加边属性

### 2. SG → RM 转换

```java
public void addSGToRM()
```

**功能**：
- 遍历 OGDataset 中的所有语句
- OGPropertyStatement 转换为节点/边的列值
- OGRelationshipStatement 转换为边行
- 使用 Schema 定义来确定列名和标签

**处理流程**：
1. 处理所有 OGPropertyStatement
   - 节点的属性语句 → 节点行的列值
   - 边的属性语句 → 边行的列值
2. 处理所有 OGRelationshipStatement
   - 创建边行
   - 设置 out/in 列值
3. 确保所有引用的节点都有对应的行

## 使用示例

### 在 ConverterUtils 中的应用

```java
// RM → SG
public static OGDataset buildSGFromRM(RMGraph rmGraph) {
    OGDataset ogDataset = new OGDataset();
    RMSGConverter converter = new RMSGConverter(rmGraph, ogDataset);
    converter.addRMToSG();
    return ogDataset;
}

// SG → RM
public static RMGraph buildRMFromSG(OGDataset ogDataset, Map<String, Schema> schemas) {
    RMGraph rmGraph = new RMGraph();
    rmGraph.setSchemas(schemas);
    RMSGConverter converter = new RMSGConverter(rmGraph, ogDataset);
    converter.addSGToRM();
    return rmGraph;
}
```

### 直接使用示例

```java
// RM → SG 转换
RMGraph rmGraph = loadRMGraph();
OGDataset ogDataset = new OGDataset();
RMSGConverter converter = new RMSGConverter(rmGraph, ogDataset);
converter.addRMToSG();

// SG → RM 转换
OGDataset ogDataset = loadOGDataset();
Map<String, Schema> schemas = defineSchemas();
RMGraph rmGraph = new RMGraph();
rmGraph.setSchemas(schemas);
RMSGConverter converter = new RMSGConverter(rmGraph, ogDataset);
converter.addSGToRM();
```

## 关键实现细节

### 1. IRI 命名空间

使用标准的命名空间常量：
- `IRINamespace.IRI_NAMESPACE`: 节点 IRI
- `IRINamespace.PROPERTIES_NAMESPACE`: 属性谓词
- `IRINamespace.EDGE_NAMESPACE`: 边谓词

### 2. ID 映射

- **RM → SG**: 使用 `Map<String, OGSimpleNodeIRI>` 缓存已创建的节点
- **SG → RM**: 使用 `Map<String, Line>` 缓存已创建的行

### 3. 边 ID 处理

- 如果边有明确的 ID（`line.getId()` 或 `relStmt.edgeIDLPG`），使用该 ID
- 否则，生成格式：`outId + "->" + edgeLabel + "->" + inId`

### 4. Schema 处理

- **节点识别**: `schema == null || schema.isNode()`
- **边识别**: `schema != null && !schema.isNode()`
- **列名映射**: 使用 schema 中定义的 `out` 和 `in` 列名
- **标签映射**: 使用 schema 中定义的 `outLabel` 和 `inLabel`

### 5. 标签前缀处理

支持 `label:id` 格式的 ID：
```java
private String stripLabelPrefix(String id, String label) {
    if (id.startsWith(label + ":")) {
        return id.substring(label.length() + 1);
    }
    return id;
}
```

### 6. 类型转换

- **Literal → Object**: 使用 `LiteralConverter.convertToObject()`
- **Object → Literal**: 使用 `LiteralConverter.convertToLiteral()`
- **OGValue → Object**: 通过 `getLinkedComponent()` 获取底层值

## 与 RMMGConverter 的对比

| 特性 | RMMGConverter | RMSGConverter |
|------|---------------|---------------|
| 目标模型 | MGraph | OGDataset |
| 节点表示 | IRI (Resource) | OGSimpleNodeIRI |
| 属性表示 | Statement (subject, pred, literal) | OGPropertyStatement |
| 边表示 | Statement (subject, edge_pred, object) | OGRelationshipStatement |
| 边属性 | Statement (edge_stmt, pred, literal) | OGPropertyStatement (edge as subject) |
| 类型系统 | RDF4J Model | OneGraph Model |

## 优势与特点

### 1. **完整的双向转换**
- 支持 RM → SG 和 SG → RM 的完整转换
- 保留所有节点、边和属性信息

### 2. **Schema 感知**
- 使用 Schema 定义来指导转换
- 支持自定义列名和标签映射

### 3. **ID 保留**
- 尽可能保留原始 ID 信息
- 支持 `label:id` 格式的复合 ID

### 4. **类型安全**
- 使用 OneGraph 的强类型系统
- 明确区分节点和边

### 5. **缓存优化**
- IRI 缓存避免重复创建
- 节点/行映射提高查找效率

## 注意事项

### 1. Schema 定义
在 SG → RM 转换时，建议提供完整的 Schema 定义，以确保：
- 正确的列名映射
- 正确的标签识别
- 正确的 out/in 列命名

### 2. 边属性处理
边属性在 OneGraph 中表示为以边为主语的 PropertyStatement，需要特殊处理。

### 3. 不完整的边
如果边缺少 out 或 in 值，转换会安全跳过该边。

### 4. 默认值
- 默认表名: `"default_table"`
- 默认 out 列名: `edgeLabel + "_out"`
- 默认 in 列名: `edgeLabel + "_in"`

## 测试建议

### 单元测试场景

1. **简单节点转换**
   - 创建包含节点的 RMGraph
   - 转换为 OGDataset
   - 验证节点和属性

2. **边转换**
   - 创建包含边的 RMGraph
   - 转换为 OGDataset
   - 验证关系语句

3. **边属性转换**
   - 创建带属性的边
   - 验证边属性正确转换

4. **双向转换一致性**
   - RM → SG → RM
   - 验证数据一致性

5. **Schema 驱动转换**
   - 使用自定义 Schema
   - 验证列名和标签映射

## 扩展可能性

### 1. 批量转换优化
可以添加批量处理模式，提高大规模数据转换效率。

### 2. 增量转换
支持增量添加数据，而不是每次都完全转换。

### 3. 转换配置
类似 LPGMappingConfiguration，可以添加转换配置选项。

### 4. 错误处理
增强错误处理和验证机制，提供更详细的错误信息。

## 总结

`RMSGConverter` 为 RM 和 OneGraph (SG) 之间提供了完整的双向转换能力，设计上参照了 `RMMGConverter`，确保了一致性和可维护性。通过 Schema 感知和类型安全的转换，它能够准确地在关系模型和图模型之间进行数据转换。

