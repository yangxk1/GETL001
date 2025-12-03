# RMGraph → UnifiedGraph → RMGraph 转换中数据丢失分析

## 问题描述
在 `RMGraph → UnifiedGraph → RMGraph` 的双向转换过程中，转换后的 RMGraph 中的 lines 数量会减少。

## 根本原因分析

### 1. 关键问题：边(Edge)转换中的处理逻辑

在 `RMConverter.java` 的 `trans2Pair` 方法中（第72-113行），当处理边(edge)时：

```java
} else {
    //edge
    IRI edgeLabel = unifiedGraph.getOrRegisterBaseIRI(EDGE_NAMESPACE_ID, schema.getTableName());
    String outId = Optional.of(line).map(Line::getValues).map(m -> m.get(schema.getOut())).map(id -> schema.getOutLabel() + ":" + id).orElse(null);
    if (outId == null) {
        return null;  // ⚠️ 问题1：返回null，这条边不会被添加到UnifiedGraph
    }
    Line out = Optional.of(outId).map(rmGraph.getLines()::get).orElse(null);
    if (out == null) {
        out = new Line();
        out.setId(outId);
        out.setTableName(schema.getOutLabel());
    }
    String inId = Optional.of(line).map(Line::getValues).map(m -> m.get(schema.getIn())).map(id -> schema.getInLabel() + ":" + id).orElse(null);
    if (inId == null) {
        return null;  // ⚠️ 问题2：返回null，这条边不会被添加到UnifiedGraph
    }
    // ...
}
```

**问题点**：
- 如果边的 `outId` 为 null（即边表中缺少 out 列的值），返回 null
- 如果边的 `inId` 为 null（即边表中缺少 in 列的值），返回 null
- 返回 null 意味着该边的 Pair 没有被创建，也就不会被添加到 UnifiedGraph 中

### 2. 从 UnifiedGraph 转回 RMGraph 的逻辑

在 `addUGMToRMModel` 方法中（第116-122行）：

```java
public void addUGMToRMModel() {
    for (BasePair basePair : unifiedGraph.getIRI2BasePair().values()) {
        handleBasePair(basePair);
    }
    for (NestedPair nestedPair : unifiedGraph.getPairs()) {
        handleLine(nestedPair);
    }
}
```

转换回 RMGraph 时：
1. 只处理 UnifiedGraph 中的 BasePair（节点）
2. 只处理 UnifiedGraph 中的 NestedPair（边和属性）

**关键**：如果在 RMGraph → UnifiedGraph 转换时某些边因为 outId 或 inId 为 null 而没有被添加到 UnifiedGraph，那么在转回 RMGraph 时这些边就会丢失。

### 3. handleLine(NestedPair) 中的 Schema 依赖

在 `handleLine(NestedPair nestedPair)` 方法中（第145-194行）：

```java
private Line handleLine(NestedPair nestedPair) {
    Line line = rmGraph.getLines().get(nestedPair.getID());
    if (line != null) {
        return line;
    }
    //subject is node
    String label = nestedPair.from().from().iterator().next().getLocalName();
    Pair inPair = nestedPair.to().to();
    BasePair outPair = nestedPair.to().from();
    Line out = handleBasePair(outPair);
    //edge
    if (rmGraph.getSchemas().containsKey(label)) {  // ⚠️ 问题3：依赖于 schemas
        Schema schema = rmGraph.getSchemas().get(label);
        // ...创建边的 Line
    }
    //property
    // ...处理属性
    out.addValue(label, value);
    return out;
}
```

**问题点**：
- 在从 UnifiedGraph 转回 RMGraph 时，需要依赖原始的 `schemas` 信息
- 如果新的 `rmGraph1` 没有正确初始化 schemas，某些 NestedPair 可能无法正确转换为 Edge Line
- 属性(property)的 NestedPair 不会创建新的 Line，而是将值添加到现有的节点 Line 中

### 4. 数据丢失的具体场景

#### 场景A：边数据不完整
```
原始数据：
- Line(id=edge1, tableName=KNOWS, values={out=null, in=person2, since=2020})
  
转换过程：
1. RMGraph → UnifiedGraph: trans2Pair 返回 null（因为 outId == null）
2. 该边不会被添加到 UnifiedGraph
3. UnifiedGraph → RMGraph: 该边不存在于 UnifiedGraph，无法恢复

结果：丢失 1 条 line
```

#### 场景B：孤立边（引用的节点不存在）
```
原始数据：
- Line(id=edge1, tableName=KNOWS, values={out=person1, in=person99})
- person99 在 rmGraph.lines 中不存在
  
转换过程：
1. RMGraph → UnifiedGraph: 会创建临时的 Line 对象（out/in），但这些临时对象可能没有完整信息
2. 可能导致转换不完整

结果：可能丢失或数据不完整
```

#### 场景C：Schema 缺失
```
原始数据：
- Line(id=line1, tableName=SOME_TABLE)
- 但 rmGraph.schemas 中没有 SOME_TABLE 的定义
  
转换过程：
1. RMGraph → UnifiedGraph: schema 为 null，按 node 处理
2. UnifiedGraph → RMGraph: 如果 rmGraph1.schemas 中没有这个 schema，无法正确还原

结果：数据结构可能改变（边变节点）
```

## 数据丢失的数量关系

根据代码逻辑，丢失的 lines 数量等于：

```
丢失的lines = 原始边中outId或inId为null的数量 + 其他转换异常的数量
```

具体包括：
1. **边数据不完整**：边表中 out 或 in 列值为 null 的记录
2. **属性转换**：属性(property)的 NestedPair 不会创建独立的 Line，而是作为节点的属性
3. **重复ID处理**：如果有 ID 冲突，可能会合并处理

## 验证方法

要准确定位丢失的数据，建议添加以下调试代码：

```java
// 在 trans2Pair 方法中，当返回 null 时记录
if (outId == null || inId == null) {
    System.out.println("边转换失败: Line ID=" + line.getId() + 
        ", tableName=" + line.getTableName() +
        ", outId=" + outId + ", inId=" + inId);
    return null;
}
```

## 解决方案建议

### 方案1：数据完整性检查
在转换前验证数据：
```java
// 检查所有边的 in/out 是否完整
for (Line line : rmGraph.getLines().values()) {
    Schema schema = rmGraph.getSchemas().get(line.getTableName());
    if (schema != null && !schema.isNode()) {
        Object outValue = line.getValues().get(schema.getOut());
        Object inValue = line.getValues().get(schema.getIn());
        if (outValue == null || inValue == null) {
            logger.warn("边数据不完整: " + line.getId());
        }
    }
}
```

### 方案2：容错处理
修改 `trans2Pair` 方法，对于不完整的边进行特殊处理而不是直接返回 null。

### 方案3：Schema 传递
在创建新的 RMGraph 时，传递原始的 schemas：
```java
public static RMGraph buildRMFromUGGraph(UnifiedGraph unifiedGraph, Map<String, Schema> originalSchemas) {
    RMGraph rmGraph = new RMGraph();
    rmGraph.setSchemas(originalSchemas);  // 保留原始 schema 信息
    RMConverter converter = new RMConverter(unifiedGraph, rmGraph);
    converter.addUGMToRMModel();
    return rmGraph;
}
```

## 总结

**核心原因**：在 RMGraph → UnifiedGraph 转换过程中，`trans2Pair` 方法对于 out/in 值为 null 的边会返回 null，导致这些边不会被添加到 UnifiedGraph 中。当从 UnifiedGraph 转回 RMGraph 时，这些丢失的边无法恢复，从而造成 lines 数量减少。

**建议的优先级**：
1. **立即**：添加日志输出，确认具体是哪些 lines 丢失了
2. **短期**：实现数据完整性检查，在转换前识别问题数据
3. **长期**：优化转换逻辑，提供更好的容错机制

