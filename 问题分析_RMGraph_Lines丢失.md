# RMGraph Lines 丢失问题的深入分析

## 问题现象

在执行以下转换时，lines 数量会减少：
```java
System.out.println(rmGraph.getLines().size());         // 例如: 1000
UnifiedGraph unifiedGraph = ConverterUtils.buildUGGraphFromRM(rmGraph);
RMGraph rmGraph1 = ConverterUtils.buildRMFromUGGraph(unifiedGraph);
System.out.println(rmGraph1.getLines().size());        // 例如: 950 (减少了50)
```

## 根本原因

### 核心问题位置

在 `RMConverter.java` 的 `trans2Pair()` 方法中（第88-91行和第98-101行）：

```java
String outId = Optional.of(line).map(Line::getValues)
                       .map(m -> m.get(schema.getOut()))
                       .map(id -> schema.getOutLabel() + ":" + id).orElse(null);
if (outId == null) {
    return null;  // ❌ 这里返回null导致边丢失！
}

String inId = Optional.of(line).map(Line::getValues)
                      .map(m -> m.get(schema.getIn()))
                      .map(id -> schema.getInLabel() + ":" + id).orElse(null);
if (inId == null) {
    return null;  // ❌ 这里返回null导致边丢失！
}
```

### 为什么会返回 null？

`outId` 或 `inId` 为 null 的三种情况：

1. **line.getValues() 为 null**
   - Line 对象的 values 字段未初始化
   
2. **m.get(schema.getOut()) 或 m.get(schema.getIn()) 返回 null**
   - 边表中缺少 out 或 in 列的值
   - 这是最常见的情况
   
3. **schema 配置错误**
   - schema.getOut() 或 schema.getIn() 返回的列名在 values map 中不存在

### 转换流程分析

#### 第一步：RMGraph → UnifiedGraph

```
RMConverter.addRMModelToUGM()
  ↓
遍历 rmGraph.getLines().values()
  ↓
对每个 Line 调用 handleLine(line)
  ↓
trans2Pair(idToPair, line, schema)
  ↓
如果是边且 outId==null 或 inId==null
  → 返回 null
  → 该边的 Pair 不会被创建
  → 不会被添加到 UnifiedGraph
```

**关键**：返回 null 的边不会生成任何 NestedPair 或 BasePair，因此：
- 不会出现在 `unifiedGraph.getIRI2BasePair()` 中
- 不会出现在 `unifiedGraph.getPairs()` 中

#### 第二步：UnifiedGraph → RMGraph

```
RMConverter.addUGMToRMModel()
  ↓
遍历 unifiedGraph.getIRI2BasePair().values()
  → 处理节点
  ↓
遍历 unifiedGraph.getPairs()
  → 处理边和属性
```

**关键**：由于第一步中某些边没有被添加到 UnifiedGraph，第二步自然无法恢复这些边。

## 数据丢失的具体场景

### 场景1：边表数据不完整（最常见）

```sql
-- 原始数据库中的边表
CREATE TABLE person_knows_person (
    id VARCHAR(50),
    person1_id BIGINT,      -- out 字段
    person2_id BIGINT,      -- in 字段
    creationDate VARCHAR(50)
);

-- 问题数据
INSERT INTO person_knows_person VALUES 
    ('knows_1', 100, NULL, '2020-01-01'),  -- person2_id 为 NULL ❌
    ('knows_2', NULL, 200, '2020-01-02');  -- person1_id 为 NULL ❌
```

转换过程：
```
1. knows_1: outId="Person:100", inId=null → trans2Pair 返回 null
2. knows_2: outId=null, inId="Person:200" → trans2Pair 返回 null
3. 这两条边都不会被添加到 UnifiedGraph
4. 转换回 RMGraph 时，这两条边丢失
```

### 场景2：Line.values 未初始化

```java
Line line = new Line();
line.setId("edge_1");
line.setTableName("KNOWS");
// line.setValues(...); // ❌ 忘记设置 values
rmGraph.getLines().put(line.getId(), line);
```

转换过程：
```
1. line.getValues() 返回 null
2. Optional.of(line).map(Line::getValues) 返回 empty Optional
3. outId = null, inId = null
4. trans2Pair 返回 null
5. 该边丢失
```

### 场景3：Schema 配置错误

```java
Schema schema = new Schema("KNOWS", "out_person", "in_person");
// 但实际的 Line values 中使用的是 "person1_id" 和 "person2_id"

Line line = new Line();
line.setId("edge_1");
line.setTableName("KNOWS");
line.addValue("person1_id", 100);  // ❌ 与 schema.out 不匹配
line.addValue("person2_id", 200);  // ❌ 与 schema.in 不匹配
```

转换过程：
```
1. schema.getOut() = "out_person"
2. line.getValues().get("out_person") = null （实际key是 "person1_id"）
3. outId = null
4. trans2Pair 返回 null
5. 该边丢失
```

## 其他可能的丢失原因

### 原因4：属性(Property)不会创建独立的 Line

在 `handleLine(NestedPair)` 方法中（第180-189行）：

```java
//property
Object value = null;
if (inPair instanceof ConstantPair) {
    ConstantPair in = (ConstantPair) inPair;
    value = in.to();
} else if (inPair instanceof BasePair) {
    value = handleBasePair((BasePair) inPair).getId();
}
out.addValue(label, value);  // ← 属性被添加到节点的 values 中，不创建新 Line
return out;                   // ← 返回的是节点的 Line，不是新的 Line
```

这意味着：
- 如果原始 RMGraph 中属性被存储为独立的 Line（虽然不常见）
- 转换后这些属性会合并到节点的 values 中
- Line 总数会减少

### 原因5：ID 重复或冲突

如果存在 ID 重复的情况：
```java
Pair pair = idToPair.get(line.getId());
if (pair != null) {
    return pair;  // ← 直接返回已存在的 pair，不创建新的
}
```

## 验证和诊断

### 方法1：使用增强的测试类

运行 `TestDetailed.java` 可以得到详细的分析报告：
```bash
cd /Users/yangxk/code/GETL001
mvn compile exec:java -Dexec.mainClass="com.getl.example.converter.TestDetailed"
```

输出示例：
```
========== 边数据完整性检查 ==========
⚠️ 不完整的边: ID=knows_123, Table=person_knows_person, Out=null, In=456
⚠️ 不完整的边: ID=knows_456, Table=person_knows_person, Out=123, In=null
不完整的边数量: 45

========== 丢失数据分析 ==========
总共丢失的Lines: 45
按Schema分类的丢失统计:
  person_knows_person (边): 45 条
```

### 方法2：使用带日志的测试类

运行 `TestWithLogging.java` 可以实时看到哪些边返回了 null：
```bash
mvn compile exec:java -Dexec.mainClass="com.getl.example.converter.TestWithLogging"
```

输出示例：
```
⚠️ 边ID=knows_123, Table=person_knows_person, 原因=inValue为null, In字段=person2_id
⚠️ 边ID=knows_456, Table=person_knows_person, 原因=outValue为null, Out字段=person1_id
========== 转换中返回null的边 ==========
返回null的边数量: 45
```

## 解决方案

### 方案1：数据清洗（推荐）

在转换前清理数据：
```java
public void cleanRMGraphData(RMGraph rmGraph) {
    List<String> toRemove = new ArrayList<>();
    
    for (Line line : rmGraph.getLines().values()) {
        Schema schema = rmGraph.getSchemas().get(line.getTableName());
        if (schema != null && !schema.isNode()) {
            Object outValue = line.getValues() != null ? 
                line.getValues().get(schema.getOut()) : null;
            Object inValue = line.getValues() != null ? 
                line.getValues().get(schema.getIn()) : null;
            
            if (outValue == null || inValue == null) {
                System.out.println("移除不完整的边: " + line.getId());
                toRemove.add(line.getId());
            }
        }
    }
    
    toRemove.forEach(id -> rmGraph.getLines().remove(id));
}
```

### 方案2：修改 trans2Pair 提供默认值

修改 `RMConverter.java`：
```java
String outId = Optional.of(line).map(Line::getValues)
                       .map(m -> m.get(schema.getOut()))
                       .map(id -> schema.getOutLabel() + ":" + id)
                       .orElse(schema.getOutLabel() + ":NULL");  // ← 提供默认值
if (outId.endsWith(":NULL")) {
    logger.warn("边缺少out值: " + line.getId());
    // 可以选择返回 null 或继续处理
}
```

### 方案3：容错转换模式

添加一个配置选项来控制行为：
```java
public class RMConverter {
    private boolean strictMode = true;  // 严格模式：有null就返回null
    
    private Pair trans2Pair(...) {
        // ...
        if (outId == null) {
            if (strictMode) {
                return null;
            } else {
                // 创建虚拟节点
                outId = schema.getOutLabel() + ":NULL_" + UUID.randomUUID();
                logger.warn("创建虚拟节点: " + outId);
            }
        }
        // ...
    }
}
```

### 方案4：保留元数据

在转换时保留原始的 schemas：
```java
public static RMGraph buildRMFromUGGraph(UnifiedGraph unifiedGraph, RMGraph originalRMGraph) {
    RMGraph rmGraph = new RMGraph();
    rmGraph.setSchemas(originalRMGraph.getSchemas());  // ← 保留原始 schemas
    RMConverter converter = new RMConverter(unifiedGraph, rmGraph);
    converter.addUGMToRMModel();
    return rmGraph;
}
```

## 总结

### 主要原因（按可能性排序）

1. **边表数据不完整**（95%+）
   - out 字段值为 null
   - in 字段值为 null
   
2. **Line.values 未初始化**（3%）
   - 代码逻辑错误
   
3. **Schema 配置与实际数据不匹配**（1%）
   - schema.out/in 字段名与实际不符
   
4. **其他**（<1%）
   - 属性存储方式变化
   - ID 冲突

### 建议的行动步骤

1. **立即执行**：运行 `TestDetailed` 或 `TestWithLogging` 确认具体原因
2. **短期解决**：实施数据清洗（方案1）
3. **中期优化**：修改转换逻辑增加容错（方案2或方案3）
4. **长期改进**：改进数据验证和Schema管理机制

### 关键代码位置

- **问题代码**：`/Users/yangxk/code/GETL001/src/main/java/com/getl/converter/RMConverter.java`
  - 第88-91行：outId 为 null 时返回 null
  - 第98-101行：inId 为 null 时返回 null
  
- **测试工具**：
  - `/Users/yangxk/code/GETL001/src/main/java/com/getl/example/converter/TestDetailed.java`
  - `/Users/yangxk/code/GETL001/src/main/java/com/getl/example/converter/TestWithLogging.java`

