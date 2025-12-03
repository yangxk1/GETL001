# GetlLogger Usage Example

## New Features

The `GetlLogger` class now includes:

1. **Automatic caching** of running time and memory consumption for each info type
2. **Statistics calculation** with the `close()` method (similar to a destructor)
3. **Programmatic access** to statistics via `getStatistics(String info)` method

## Usage Example

```java
import com.getl.util.GetlLogger;
import java.util.Map;

public class LoggerExample {
    public static void main(String[] args) {
        // Create logger
        GetlLogger logger = new GetlLogger("my_test");
        
        // Log same operation multiple times
        for (int i = 0; i < 5; i++) {
            long startTime = System.currentTimeMillis();
            
            // Simulate some work
            doSomeWork();
            
            long endTime = System.currentTimeMillis();
            long timeUsed = endTime - startTime;
            
            // Log with the same info name
            logger.debugInfo("Process Data", timeUsed);
        }
        
        // Log another operation
        for (int i = 0; i < 3; i++) {
            long startTime = System.currentTimeMillis();
            
            loadDatabase();
            
            long endTime = System.currentTimeMillis();
            logger.debugInfo("Load Database", endTime - startTime);
        }
        
        // Get statistics programmatically (optional)
        Map<String, Object> stats = logger.getStatistics("Process Data");
        if (stats != null) {
            System.out.println("Average time for 'Process Data': " + stats.get("avgTime") + " ms");
            System.out.println("Average memory for 'Process Data': " + stats.get("avgMemory") + " bytes");
            System.out.println("Execution count: " + stats.get("count"));
        }
        
        // Call close() to generate statistics summary
        // This will output a comprehensive statistics report for all logged operations
        logger.close();
    }
    
    private static void doSomeWork() {
        // Your code here
    }
    
    private static void loadDatabase() {
        // Your code here
    }
}
```

## Output Format

### Individual Log Entry
```
## Process Data
```
current time: 2025-12-03 14:30:15.123
used time: 1,234 ms
JVM max Memory (Byte): 4,294,967,296 B
JVM current total Memory (Byte): 2,147,483,648 B
JVM current free Memory (Byte): 1,073,741,824 B
Used Memory (Byte): 1,073,741,824 B
```
```

### Statistics Summary (after calling close())
```
---
# Statistics Summary

## Load Database
```
Execution count: 3

Time Statistics:
  Average time: 850 ms
  Min time: 800 ms
  Max time: 900 ms
  Total time: 2,550 ms

Memory Statistics:
  Average memory: 1,100,000,000 B
  Min memory: 1,050,000,000 B
  Max memory: 1,150,000,000 B
  Max memory (MB): 1097.15 MB
```

## Process Data
```
Execution count: 5

Time Statistics:
  Average time: 1,200 ms
  Min time: 1,000 ms
  Max time: 1,400 ms
  Total time: 6,000 ms

Memory Statistics:
  Average memory: 1,073,741,824 B
  Min memory: 1,000,000,000 B
  Max memory: 1,200,000,000 B
  Max memory (MB): 1144.41 MB
```
```

## Key Features

- **Automatic Grouping**: All logs with the same `info` string are automatically grouped together
- **Comprehensive Statistics**: Min, max, average, and total values for both time and memory
- **Memory in Multiple Units**: Memory is shown in both bytes and MB for convenience
- **Sorted Output**: Statistics are sorted alphabetically by info type for easy reading
- **File Output**: All statistics are written to the log file specified in the constructor

## Important Notes

1. **Call close()**: Remember to call `logger.close()` at the end of your program to generate the statistics summary
2. **Same Info String**: Use the exact same `info` string for operations you want to group together
3. **Memory Measurement**: Memory consumption is measured at the time of each `debugInfo()` call
4. **Thread Safety**: The current implementation is not thread-safe. For multi-threaded use, consider adding synchronization

