import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

# 高效写入

本节介绍如何高效地向 TDengine 写入数据。

## 高效写入原理 {#principle}

### 客户端程序的角度 {#application-view}

从客户端程序的角度来说，高效写入数据要考虑以下几个因素：

1. 单次写入的数据量。一般来讲，每批次写入的数据量越大越高效（但超过一定阈值其优势会消失）。使用 SQL 写入 TDengine 时，尽量在一条 SQL 中拼接更多数据。目前，TDengine 支持的一条 SQL 的最大长度为 1,048,576（1MB）个字符
2. 并发连接数。一般来讲，同时写入数据的并发连接数越多写入越高效（但超过一定阈值反而会下降，取决于服务端处理能力）
3. 数据在不同表（或子表）之间的分布，即要写入数据的相邻性。一般来说，每批次只向同一张表（或子表）写入数据比向多张表（或子表）写入数据要更高效
4. 写入方式。一般来讲：
   - 参数绑定写入比 SQL 写入更高效。因参数绑定方式避免了 SQL 解析。（但增加了 C 接口的调用次数，对于连接器也有性能损耗）
   - SQL 写入不自动建表比自动建表更高效。因自动建表要频繁检查表是否存在
   - SQL 写入比无模式写入更高效。因无模式写入会自动建表且支持动态更改表结构

客户端程序要充分且恰当地利用以上几个因素。在单次写入中尽量只向同一张表（或子表）写入数据，每批次写入的数据量经过测试和调优设定为一个最适合当前系统处理能力的数值，并发写入的连接数同样经过测试和调优后设定为一个最适合当前系统处理能力的数值，以实现在当前系统中的最佳写入速度。

### 数据源的角度 {#datasource-view}

客户端程序通常需要从数据源读数据再写入 TDengine。从数据源角度来说，以下几种情况需要在读线程和写线程之间增加队列：

1. 有多个数据源，单个数据源生成数据的速度远小于单线程写入的速度，但数据量整体比较大。此时队列的作用是把多个数据源的数据汇聚到一起，增加单次写入的数据量。
2. 单个数据源生成数据的速度远大于单线程写入的速度。此时队列的作用是增加写入的并发度。
3. 单张表的数据分散在多个数据源。此时队列的作用是将同一张表的数据提前汇聚到一起，提高写入时数据的相邻性。

如果写应用的数据源是 Kafka, 写应用本身即 Kafka 的消费者，则可利用 Kafka 的特性实现高效写入。比如：

1. 将同一张表的数据写到同一个 Topic 的同一个 Partition，增加数据的相邻性
2. 通过订阅多个 Topic 实现数据汇聚
3. 通过增加 Consumer 线程数增加写入的并发度
4. 通过增加每次 Fetch 的最大数据量来增加单次写入的最大数据量

### 服务器配置的角度 {#setting-view}

从服务端配置的角度，要根据系统中磁盘的数量，磁盘的 I/O 能力，以及处理器能力在创建数据库时设置适当的 vgroups 数量以充分发挥系统性能。如果 vgroups 过少，则系统性能无法发挥；如果 vgroups 过多，会造成无谓的资源竞争。常规推荐 vgroups 数量为 CPU 核数的 2 倍，但仍然要结合具体的系统资源配置进行调优。

更多调优参数，请参考 [数据库管理](../../../taos-sql/database) 和 [服务端配置](../../../reference/config)。

## 高效写入示例 {#sample-code}

### 场景设计 {#scenario}

下面的示例程序展示了如何高效写入数据，场景设计如下：

- TDengine 客户端程序从其它数据源不断读入数据，在示例程序中采用生成模拟数据的方式来模拟读取数据源
- 单个连接向 TDengine 写入的速度无法与读数据的速度相匹配，因此客户端程序启动多个线程，每个线程都建立了与 TDengine 的连接，每个线程都有一个独占的固定大小的消息队列
- 客户端程序将接收到的数据根据所属的表名（或子表名）HASH 到不同的线程，即写入该线程所对应的消息队列，以此确保属于某个表（或子表）的数据一定会被一个固定的线程处理
- 各个子线程在将所关联的消息队列中的数据读空后或者读取数据量达到一个预定的阈值后将该批数据写入 TDengine，并继续处理后面接收到的数据

![TDengine 高效写入示例场景的线程模型](highvolume.webp)

### 示例代码 {#code}

这一部分是针对以上场景的示例代码。对于其它场景高效写入原理相同，不过代码需要适当修改。

本示例代码假设源数据属于同一张超级表（meters）的不同子表。程序在开始写入数据之前已经在 test 库创建了这个超级表。对于子表，将根据收到的数据，由应用程序自动创建。如果实际场景是多个超级表，只需修改写任务自动建表的代码。

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

**程序清单**

| 类名             | 功能说明                                                                    |
| ---------------- | --------------------------------------------------------------------------- |
| FastWriteExample | 主程序                                                                      |
| ReadTask         | 从模拟源中读取数据，将表名经过 Hash 后得到 Queue 的 Index，写入对应的 Queue |
| WriteTask        | 从 Queue 中获取数据，组成一个 Batch，写入 TDengine                          |
| MockDataSource   | 模拟生成一定数量 meters 子表的数据                                          |
| SQLWriter        | WriteTask 依赖这个类完成 SQL 拼接、自动建表、 SQL 写入、SQL 长度检查        |
| StmtWriter       | 实现参数绑定方式批量写入（暂未完成）                                        |
| DataBaseMonitor  | 统计写入速度，并每隔 10 秒把当前写入速度打印到控制台                        |

以下是各类的完整代码和更详细的功能说明。

<details>
<summary>FastWriteExample</summary>
主程序负责：

1. 创建消息队列
2. 启动写线程
3. 启动读线程
4. 每隔 10 秒统计一次写入速度

主程序默认暴露了 4 个参数，每次启动程序都可调节，用于测试和调优：

1. 读线程个数。默认为 1。
2. 写线程个数。默认为 3。
3. 模拟生成的总表数。默认为 1,000。将会平分给各个读线程。如果总表数较大，建表需要花费较长，开始统计的写入速度可能较慢。
4. 每批最多写入记录数量。默认为 3,000。

队列容量（taskQueueCapacity）也是与性能有关的参数，可通过修改程序调节。一般来讲，队列容量越大，入队被阻塞的概率越小，队列的吞吐量越大，但是内存占用也会越大。示例程序默认值已经设置地足够大。

```java
package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class FastWriteExample {
    final static Logger logger = LoggerFactory.getLogger(FastWriteExample.class);

    final static int taskQueueCapacity = 1000000;
    final static List<BlockingQueue<String>> taskQueues = new ArrayList<>();
    final static List<ReadTask> readTasks = new ArrayList<>();
    final static List<WriteTask> writeTasks = new ArrayList<>();
    final static DataBaseMonitor databaseMonitor = new DataBaseMonitor();

    public static void stopAll() {
        logger.info("shutting down");
        readTasks.forEach(task -> task.stop());
        writeTasks.forEach(task -> task.stop());
        databaseMonitor.close();
    }

    public static void main(String[] args) throws InterruptedException, SQLException {
        int readTaskCount = args.length > 0 ? Integer.parseInt(args[0]) : 1;
        int writeTaskCount = args.length > 1 ? Integer.parseInt(args[1]) : 3;
        int tableCount = args.length > 2 ? Integer.parseInt(args[2]) : 1000;
        int maxBatchSize = args.length > 3 ? Integer.parseInt(args[3]) : 3000;

        logger.info("readTaskCount={}, writeTaskCount={} tableCount={} maxBatchSize={}",
                readTaskCount, writeTaskCount, tableCount, maxBatchSize);

        databaseMonitor.init().prepareDatabase();

        // Create task queues, whiting tasks and start writing threads.
        for (int i = 0; i < writeTaskCount; ++i) {
            BlockingQueue<String> queue = new ArrayBlockingQueue<>(taskQueueCapacity);
            taskQueues.add(queue);
            WriteTask task = new WriteTask(queue, maxBatchSize);
            Thread t = new Thread(task);
            t.setName("WriteThread-" + i);
            t.start();
        }

        // create reading tasks and start reading threads
        int tableCountPerTask = tableCount / readTaskCount;
        for (int i = 0; i < readTaskCount; ++i) {
            ReadTask task = new ReadTask(i, taskQueues, tableCountPerTask);
            Thread t = new Thread(task);
            t.setName("ReadThread-" + i);
            t.start();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(FastWriteExample::stopAll));

        long lastCount = 0;
        while (true) {
            Thread.sleep(10000);
            long numberOfTable = databaseMonitor.getTableCount();
            long count = databaseMonitor.count();
            logger.info("numberOfTable={} count={} speed={}", numberOfTable, count, (count - lastCount) / 10);
            lastCount = count;
        }
    }
}
```

[查看源码](https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/highvolume/FastWriteExample.java)

</details>

<details>
<summary>ReadTask</summary>

读任务负责从数据源读数据。每个读任务都关联了一个模拟数据源。每个模拟数据源可生成一点数量表的数据。不同的模拟数据源生成不同表的数据。

读任务采用阻塞的方式写消息队列。也就是说，一旦队列满了，写操作就会阻塞。

```java
package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

class ReadTask implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(ReadTask.class);
    private final int taskId;
    private final List<BlockingQueue<String>> taskQueues;
    private final int queueCount;
    private final int tableCount;
    private boolean active = true;

    public ReadTask(int readTaskId, List<BlockingQueue<String>> queues, int tableCount) {
        this.taskId = readTaskId;
        this.taskQueues = queues;
        this.queueCount = queues.size();
        this.tableCount = tableCount;
    }

    /**
     * Assign data received to different queues.
     * Here we use the suffix number in table name.
     * You are expected to define your own rule in practice.
     *
     * @param line record received
     * @return which queue to use
     */
    public int getQueueId(String line) {
        String tbName = line.substring(0, line.indexOf(',')); // For example: tb1_101
        String suffixNumber = tbName.split("_")[1];
        return Integer.parseInt(suffixNumber) % this.queueCount;
    }

    @Override
    public void run() {
        logger.info("started");
        Iterator<String> it = new MockDataSource("tb" + this.taskId, tableCount);
        try {
            while (it.hasNext() && active) {
                String line = it.next();
                int queueId = getQueueId(line);
                taskQueues.get(queueId).put(line);
            }
        } catch (Exception e) {
            logger.error("Read Task Error", e);
        }
    }

    public void stop() {
        logger.info("stop");
        this.active = false;
    }
}
```

[查看源码](https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/highvolume/ReadTask.java)

</details>

<details>
<summary>WriteTask</summary>

```java
package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

class WriteTask implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(WriteTask.class);
    private final int maxBatchSize;

    // the queue from which this writing task get raw data.
    private final BlockingQueue<String> queue;

    // A flag indicate whether to continue.
    private boolean active = true;

    public WriteTask(BlockingQueue<String> taskQueue, int maxBatchSize) {
        this.queue = taskQueue;
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public void run() {
        logger.info("started");
        String line = null; // data getting from the queue just now.
        SQLWriter writer = new SQLWriter(maxBatchSize);
        try {
            writer.init();
            while (active) {
                line = queue.poll();
                if (line != null) {
                    // parse raw data and buffer the data.
                    writer.processLine(line);
                } else if (writer.hasBufferedValues()) {
                    // write data immediately if no more data in the queue
                    writer.flush();
                } else {
                    // sleep a while to avoid high CPU usage if no more data in the queue and no buffered records, .
                    Thread.sleep(100);
                }
            }
            if (writer.hasBufferedValues()) {
                writer.flush();
            }
        } catch (Exception e) {
            String msg = String.format("line=%s, bufferedCount=%s", line, writer.getBufferedCount());
            logger.error(msg, e);
        } finally {
            writer.close();
        }
    }

    public void stop() {
        logger.info("stop");
        this.active = false;
    }
}
```

[查看源码](https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/highvolume/WriteTask.java)

</details>

<details>

<summary>MockDataSource</summary>

```java
package com.taos.example.highvolume;

import java.util.Iterator;

/**
 * Generate test data
 */
class MockDataSource implements Iterator {
    private String tbNamePrefix;
    private int tableCount;
    private long maxRowsPerTable = 1000000000L;

    // 100 milliseconds between two neighbouring rows.
    long startMs = System.currentTimeMillis() - maxRowsPerTable * 100;
    private int currentRow = 0;
    private int currentTbId = -1;

    // mock values
    String[] location = {"California.LosAngeles", "California.SanDiego", "California.SanJose", "California.Campbell", "California.SanFrancisco"};
    float[] current = {8.8f, 10.7f, 9.9f, 8.9f, 9.4f};
    int[] voltage = {119, 116, 111, 113, 118};
    float[] phase = {0.32f, 0.34f, 0.33f, 0.329f, 0.141f};

    public MockDataSource(String tbNamePrefix, int tableCount) {
        this.tbNamePrefix = tbNamePrefix;
        this.tableCount = tableCount;
    }

    @Override
    public boolean hasNext() {
        currentTbId += 1;
        if (currentTbId == tableCount) {
            currentTbId = 0;
            currentRow += 1;
        }
        return currentRow < maxRowsPerTable;
    }

    @Override
    public String next() {
        long ts = startMs + 100 * currentRow;
        int groupId = currentTbId % 5 == 0 ? currentTbId / 5 : currentTbId / 5 + 1;
        StringBuilder sb = new StringBuilder(tbNamePrefix + "_" + currentTbId + ","); // tbName
        sb.append(ts).append(','); // ts
        sb.append(current[currentRow % 5]).append(','); // current
        sb.append(voltage[currentRow % 5]).append(','); // voltage
        sb.append(phase[currentRow % 5]).append(','); // phase
        sb.append(location[currentRow % 5]).append(','); // location
        sb.append(groupId); // groupID

        return sb.toString();
    }
}

```

[查看源码](https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/highvolume/MockDataSource.java)

</details>

<details>

<summary>SQLWriter</summary>

SQLWriter 类封装了拼 SQL 和写数据的逻辑。注意，所有的表都没有提前创建，而是在 catch 到表不存在异常的时候，再以超级表为模板批量建表，然后重新执行 INSERT 语句。对于其它异常，这里简单地记录当时执行的 SQL 语句到日志中，你也可以记录更多线索到日志，已便排查错误和故障恢复。

```java
package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * A helper class encapsulate the logic of writing using SQL.
 * <p>
 * The main interfaces are two methods:
 * <ol>
 *     <li>{@link SQLWriter#processLine}, which receive raw lines from WriteTask and group them by table names.</li>
 *     <li>{@link  SQLWriter#flush}, which assemble INSERT statement and execute it.</li>
 * </ol>
 * <p>
 * There is a technical skill worth mentioning: we create table as needed when "table does not exist" error occur instead of creating table automatically using syntax "INSET INTO tb USING stb".
 * This ensure that checking table existence is a one-time-only operation.
 * </p>
 *
 * </p>
 */
public class SQLWriter {
    final static Logger logger = LoggerFactory.getLogger(SQLWriter.class);

    private Connection conn;
    private Statement stmt;

    /**
     * current number of buffered records
     */
    private int bufferedCount = 0;
    /**
     * Maximum number of buffered records.
     * Flush action will be triggered if bufferedCount reached this value,
     */
    private int maxBatchSize;


    /**
     * Maximum SQL length.
     */
    private int maxSQLLength = 800_000;

    /**
     * Map from table name to column values. For example:
     * "tb001" -> "(1648432611249,2.1,114,0.09) (1648432611250,2.2,135,0.2)"
     */
    private Map<String, String> tbValues = new HashMap<>();

    /**
     * Map from table name to tag values in the same order as creating stable.
     * Used for creating table.
     */
    private Map<String, String> tbTags = new HashMap<>();

    public SQLWriter(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }


    /**
     * Get Database Connection
     *
     * @return Connection
     * @throws SQLException
     */
    private static Connection getConnection() throws SQLException {
        String jdbcURL = System.getenv("TDENGINE_JDBC_URL");
        return DriverManager.getConnection(jdbcURL);
    }

    /**
     * Create Connection and Statement
     *
     * @throws SQLException
     */
    public void init() throws SQLException {
        conn = getConnection();
        stmt = conn.createStatement();
        stmt.execute("use test");
    }

    /**
     * Convert raw data to SQL fragments, group them by table name and cache them in a HashMap.
     * Trigger writing when number of buffered records reached maxBachSize.
     *
     * @param line raw data get from task queue in format: tbName,ts,current,voltage,phase,location,groupId
     */
    public void processLine(String line) throws SQLException {
        bufferedCount += 1;
        int firstComma = line.indexOf(',');
        String tbName = line.substring(0, firstComma);
        int lastComma = line.lastIndexOf(',');
        int secondLastComma = line.lastIndexOf(',', lastComma - 1);
        String value = "(" + line.substring(firstComma + 1, secondLastComma) + ") ";
        if (tbValues.containsKey(tbName)) {
            tbValues.put(tbName, tbValues.get(tbName) + value);
        } else {
            tbValues.put(tbName, value);
        }
        if (!tbTags.containsKey(tbName)) {
            String location = line.substring(secondLastComma + 1, lastComma);
            String groupId = line.substring(lastComma + 1);
            String tagValues = "('" + location + "'," + groupId + ')';
            tbTags.put(tbName, tagValues);
        }
        if (bufferedCount == maxBatchSize) {
            flush();
        }
    }


    /**
     * Assemble INSERT statement using buffered SQL fragments in Map {@link SQLWriter#tbValues} and execute it.
     * In case of "Table does not exit" exception, create all tables in the sql and retry the sql.
     */
    public void flush() throws SQLException {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        for (Map.Entry<String, String> entry : tbValues.entrySet()) {
            String tableName = entry.getKey();
            String values = entry.getValue();
            String q = tableName + " values " + values + " ";
            if (sb.length() + q.length() > maxSQLLength) {
                executeSQL(sb.toString());
                logger.warn("increase maxSQLLength or decrease maxBatchSize to gain better performance");
                sb = new StringBuilder("INSERT INTO ");
            }
            sb.append(q);
        }
        executeSQL(sb.toString());
        tbValues.clear();
        bufferedCount = 0;
    }

    private void executeSQL(String sql) throws SQLException {
        try {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            // convert to error code defined in taoserror.h
            int errorCode = e.getErrorCode() & 0xffff;
            if (errorCode == 0x2603) {
                // Table does not exist
                createTables();
                executeSQL(sql);
            } else {
                logger.error("Execute SQL: {}", sql);
                throw e;
            }
        } catch (Throwable throwable) {
            logger.error("Execute SQL: {}", sql);
            throw throwable;
        }
    }

    /**
     * Create tables in batch using syntax:
     * <p>
     * CREATE TABLE [IF NOT EXISTS] tb_name1 USING stb_name TAGS (tag_value1, ...) [IF NOT EXISTS] tb_name2 USING stb_name TAGS (tag_value2, ...) ...;
     * </p>
     */
    private void createTables() throws SQLException {
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        for (String tbName : tbValues.keySet()) {
            String tagValues = tbTags.get(tbName);
            sb.append("IF NOT EXISTS ").append(tbName).append(" USING meters TAGS ").append(tagValues).append(" ");
        }
        String sql = sb.toString();
        try {
            stmt.executeUpdate(sql);
        } catch (Throwable throwable) {
            logger.error("Execute SQL: {}", sql);
            throw throwable;
        }
    }

    public boolean hasBufferedValues() {
        return bufferedCount > 0;
    }

    public int getBufferedCount() {
        return bufferedCount;
    }

    public void close() {
        try {
            stmt.close();
        } catch (SQLException e) {
        }
        try {
            conn.close();
        } catch (SQLException e) {
        }
    }
}
```

[查看源码](https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/highvolume/SQLWriter.java)

</details>

<details>

<summary>DataBaseMonitor</summary>

```java
package com.taos.example.highvolume;

import java.sql.*;

/**
 * Prepare target database.
 * Count total records in database periodically so that we can estimate the writing speed.
 */
public class DataBaseMonitor {
    private Connection conn;
    private Statement stmt;

    public DataBaseMonitor init() throws SQLException {
        if (conn == null) {
            String jdbcURL = System.getenv("TDENGINE_JDBC_URL");
            conn = DriverManager.getConnection(jdbcURL);
            stmt = conn.createStatement();
        }
        return this;
    }

    public void close() {
        try {
            stmt.close();
        } catch (SQLException e) {
        }
        try {
            conn.close();
        } catch (SQLException e) {
        }
    }

    public void prepareDatabase() throws SQLException {
        stmt.execute("DROP DATABASE IF EXISTS test");
        stmt.execute("CREATE DATABASE test");
        stmt.execute("CREATE STABLE test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
    }

    public long count() throws SQLException {
        try (ResultSet result = stmt.executeQuery("SELECT count(*) from test.meters")) {
            result.next();
            return result.getLong(1);
        }
    }

    public long getTableCount() throws SQLException {
        try (ResultSet result = stmt.executeQuery("select count(*) from information_schema.ins_tables where db_name = 'test';")) {
            result.next();
            return result.getLong(1);
        }
    }
}
```

[查看源码](https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/highvolume/DataBaseMonitor.java)

</details>

**执行步骤**

<details>
<summary>执行 Java 示例程序</summary>

执行程序前需配置环境变量 `TDENGINE_JDBC_URL`。如果 TDengine Server 部署在本机，且用户名、密码和端口都是默认值，那么可配置：

```
TDENGINE_JDBC_URL="jdbc:TAOS://localhost:6030?user=root&password=taosdata"
```

**本地集成开发环境执行示例程序**

1. clone TDengine 仓库
   ```
   git clone git@github.com:taosdata/TDengine.git --depth 1
   ```
2. 用集成开发环境打开 `docs/examples/java` 目录。
3. 在开发环境中配置环境变量 `TDENGINE_JDBC_URL`。如果已配置了全局的环境变量 `TDENGINE_JDBC_URL` 可跳过这一步。
4. 运行类 `com.taos.example.highvolume.FastWriteExample`。

**远程服务器上执行示例程序**

若要在服务器上执行示例程序，可按照下面的步骤操作：

1. 打包示例代码。在目录 TDengine/docs/examples/java 下执行：
   ```
   mvn package
   ```
2. 远程服务器上创建 examples 目录：
   ```
   mkdir -p examples/java
   ```
3. 复制依赖到服务器指定目录：
   - 复制依赖包，只用复制一次
     ```
     scp -r .\target\lib <user>@<host>:~/examples/java
     ```
   - 复制本程序的 jar 包，每次更新代码都需要复制
     ```
     scp -r .\target\javaexample-1.0.jar <user>@<host>:~/examples/java
     ```
4. 配置环境变量。
   编辑 `~/.bash_profile` 或 `~/.bashrc` 添加如下内容例如：

   ```
   export TDENGINE_JDBC_URL="jdbc:TAOS://localhost:6030?user=root&password=taosdata"
   ```

   以上使用的是本地部署 TDengine Server 时默认的 JDBC URL。你需要根据自己的实际情况更改。

5. 用 Java 命令启动示例程序，命令模板：

   ```
   java -classpath lib/*:javaexample-1.0.jar  com.taos.example.highvolume.FastWriteExample <read_thread_count>  <white_thread_count> <total_table_count> <max_batch_size>
   ```

6. 结束测试程序。测试程序不会自动结束，在获取到当前配置下稳定的写入速度后，按 <kbd>CTRL</kbd> + <kbd>C</kbd> 结束程序。
   下面是一次实际运行的日志输出，机器配置 16 核 + 64G + 固态硬盘。

   ```
   root@vm85$ java -classpath lib/*:javaexample-1.0.jar  com.taos.example.highvolume.FastWriteExample 2 12
   18:56:35.896 [main] INFO  c.t.e.highvolume.FastWriteExample - readTaskCount=2, writeTaskCount=12 tableCount=1000 maxBatchSize=3000
   18:56:36.011 [WriteThread-0] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.015 [WriteThread-0] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.021 [WriteThread-1] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.022 [WriteThread-1] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.031 [WriteThread-2] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.032 [WriteThread-2] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.041 [WriteThread-3] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.042 [WriteThread-3] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.093 [WriteThread-4] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.094 [WriteThread-4] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.099 [WriteThread-5] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.100 [WriteThread-5] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.100 [WriteThread-6] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.101 [WriteThread-6] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.103 [WriteThread-7] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.104 [WriteThread-7] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.105 [WriteThread-8] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.107 [WriteThread-8] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.108 [WriteThread-9] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.109 [WriteThread-9] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.156 [WriteThread-10] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.157 [WriteThread-11] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.158 [WriteThread-10] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.158 [ReadThread-0] INFO  com.taos.example.highvolume.ReadTask - started
   18:56:36.158 [ReadThread-1] INFO  com.taos.example.highvolume.ReadTask - started
   18:56:36.158 [WriteThread-11] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:46.369 [main] INFO  c.t.e.highvolume.FastWriteExample - count=18554448 speed=1855444
   18:56:56.946 [main] INFO  c.t.e.highvolume.FastWriteExample - count=39059660 speed=2050521
   18:57:07.322 [main] INFO  c.t.e.highvolume.FastWriteExample - count=59403604 speed=2034394
   18:57:18.032 [main] INFO  c.t.e.highvolume.FastWriteExample - count=80262938 speed=2085933
   18:57:28.432 [main] INFO  c.t.e.highvolume.FastWriteExample - count=101139906 speed=2087696
   18:57:38.921 [main] INFO  c.t.e.highvolume.FastWriteExample - count=121807202 speed=2066729
   18:57:49.375 [main] INFO  c.t.e.highvolume.FastWriteExample - count=142952417 speed=2114521
   18:58:00.689 [main] INFO  c.t.e.highvolume.FastWriteExample - count=163650306 speed=2069788
   18:58:11.646 [main] INFO  c.t.e.highvolume.FastWriteExample - count=185019808 speed=2136950
   ```

</details>

</TabItem>
<TabItem label="Python" value="python">

**程序清单**

Python 示例程序中采用了多进程的架构，并使用了跨进程的消息队列。

| 函数或类                 | 功能说明                                                             |
| ------------------------ | -------------------------------------------------------------------- |
| main 函数                | 程序入口， 创建各个子进程和消息队列                                  |
| run_monitor_process 函数 | 创建数据库，超级表，统计写入速度并定时打印到控制台                   |
| run_read_task 函数       | 读进程主要逻辑，负责从其它数据系统读数据，并分发数据到为之分配的队列 |
| MockDataSource 类        | 模拟数据源, 实现迭代器接口，每次批量返回每张表的接下来 1,000 条数据  |
| run_write_task 函数      | 写进程主要逻辑。每次从队列中取出尽量多的数据，并批量写入             |
| SQLWriter 类             | SQL 写入和自动建表                                                   |
| StmtWriter 类            | 实现参数绑定方式批量写入（暂未完成）                                 |

<details>
<summary>main 函数</summary>

main 函数负责创建消息队列和启动子进程，子进程有 3 类：

1. 1 个监控进程，负责数据库初始化和统计写入速度
2. n 个读进程，负责从其它数据系统读数据
3. m 个写进程，负责写数据库

main 函数可以接收 5 个启动参数，依次是：

1. 读任务（进程）数, 默认为 1
2. 写任务（进程）数, 默认为 1
3. 模拟生成的总表数，默认为 1,000
4. 队列大小（单位字节），默认为 1,000,000
5. 每批最多写入记录数量， 默认为 3,000

```python
def main(infinity):
    set_global_config()
    logging.info(f"READ_TASK_COUNT={READ_TASK_COUNT}, WRITE_TASK_COUNT={WRITE_TASK_COUNT}, "
                 f"TABLE_COUNT={TABLE_COUNT}, QUEUE_SIZE={QUEUE_SIZE}, MAX_BATCH_SIZE={MAX_BATCH_SIZE}")

    conn = get_connection()
    conn.execute("DROP DATABASE IF EXISTS test")
    conn.execute("CREATE DATABASE IF NOT EXISTS test keep 36500")
    conn.execute("CREATE STABLE IF NOT EXISTS test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
                 "TAGS (location BINARY(64), groupId INT)")
    conn.close()

    done_queue = Queue()
    monitor_process = Process(target=run_monitor_process, args=(done_queue,))
    monitor_process.start()
    logging.debug(f"monitor task started with pid {monitor_process.pid}")

    task_queues: List[Queue] = []
    write_processes = []
    read_processes = []

    # create task queues
    for i in range(WRITE_TASK_COUNT):
        queue = Queue()
        task_queues.append(queue)

    # create write processes
    for i in range(WRITE_TASK_COUNT):
        p = Process(target=run_write_task, args=(i, task_queues[i], done_queue))
        p.start()
        logging.debug(f"WriteTask-{i} started with pid {p.pid}")
        write_processes.append(p)

    # create read processes
    for i in range(READ_TASK_COUNT):
        queues = assign_queues(i, task_queues)
        p = Process(target=run_read_task, args=(i, queues, infinity))
        p.start()
        logging.debug(f"ReadTask-{i} started with pid {p.pid}")
        read_processes.append(p)

    try:
        monitor_process.join()
        for p in read_processes:
            p.join()
        for p in write_processes:
            p.join()
        time.sleep(1)
        return
    except KeyboardInterrupt:
        monitor_process.terminate()
        [p.terminate() for p in read_processes]
        [p.terminate() for p in write_processes]
        [q.close() for q in task_queues]


def assign_queues(read_task_id, task_queues):
    """
    Compute target queues for a specific read task.
    """
    ratio = WRITE_TASK_COUNT / READ_TASK_COUNT
    from_index = math.floor(read_task_id * ratio)
    end_index = math.ceil((read_task_id + 1) * ratio)
    return task_queues[from_index:end_index]


if __name__ == '__main__':
    multiprocessing.set_start_method('spawn')
    main(False)
```

[查看源码](https://github.com/taosdata/TDengine/blob/main/docs/examples/python/fast_write_example.py)

</details>

<details>
<summary>run_monitor_process</summary>

监控进程负责初始化数据库，并监控当前的写入速度。

```python
def run_monitor_process(done_queue: Queue):
    log = logging.getLogger("DataBaseMonitor")
    conn = None
    try:
        conn = get_connection()

        def get_count():
            res = conn.query("SELECT count(*) FROM test.meters")
            rows = res.fetch_all()
            return rows[0][0] if rows else 0

        last_count = 0
        while True:
            try:
                done = done_queue.get_nowait()
                if done == _DONE_MESSAGE:
                    break
            except Empty:
                pass
            time.sleep(10)
            count = get_count()
            log.info(f"count={count} speed={(count - last_count) / 10}")
            last_count = count
    finally:
        conn.close()


```

[查看源码](https://github.com/taosdata/TDengine/blob/main/docs/examples/python/fast_write_example.py)

</details>

<details>

<summary>run_read_task 函数</summary>

读进程，负责从其它数据系统读数据，并分发数据到为之分配的队列。

```python

def run_read_task(task_id: int, task_queues: List[Queue], infinity):
    table_count_per_task = TABLE_COUNT // READ_TASK_COUNT
    data_source = MockDataSource(f"tb{task_id}", table_count_per_task, infinity)
    try:
        for batch in data_source:
            if isinstance(batch, tuple):
                batch = [batch]
            for table_id, rows in batch:
                # hash data to different queue
                i = table_id % len(task_queues)
                # block putting forever when the queue is full
                for row in rows:
                    task_queues[i].put(row)
        if not infinity:
            for queue in task_queues:
                queue.put(_DONE_MESSAGE)
    except KeyboardInterrupt:
        pass
    finally:
        logging.info('read task over')


```

[查看源码](https://github.com/taosdata/TDengine/blob/main/docs/examples/python/fast_write_example.py)

</details>

<details>

<summary>MockDataSource</summary>

以下是模拟数据源的实现，我们假设数据源生成的每一条数据都带有目标表名信息。实际中你可能需要一定的规则确定目标表名。

```python
import time


class MockDataSource:
    samples = [
        "8.8,119,0.32,California.LosAngeles,0",
        "10.7,116,0.34,California.SanDiego,1",
        "9.9,111,0.33,California.SanJose,2",
        "8.9,113,0.329,California.Campbell,3",
        "9.4,118,0.141,California.SanFrancisco,4"
    ]

    def __init__(self, tb_name_prefix, table_count, infinity=True):
        self.table_name_prefix = tb_name_prefix + "_"
        self.table_count = table_count
        self.max_rows = 10000000
        self.current_ts = round(time.time() * 1000) - self.max_rows * 100
        # [(tableId, tableName, values),]
        self.data = self._init_data()
        self.infinity = infinity

    def _init_data(self):
        lines = self.samples * (self.table_count // 5 + 1)
        data = []
        for i in range(self.table_count):
            table_name = self.table_name_prefix + str(i)
            data.append((i, table_name, lines[i]))  # tableId, row
        return data

    def __iter__(self):
        self.row = 0
        if not self.infinity:
            return iter(self._iter_data())
        else:
            return self

    def __next__(self):
        """
        next 1000 rows for each table.
        return: {tableId:[row,...]}
        """
        return self._iter_data()

    def _iter_data(self):
        ts = []
        for _ in range(1000):
            self.current_ts += 100
            ts.append(str(self.current_ts))
        # add timestamp to each row
        # [(tableId, ["tableName,ts,current,voltage,phase,location,groupId"])]
        result = []
        for table_id, table_name, values in self.data:
            rows = [table_name + ',' + t + ',' + values for t in ts]
            result.append((table_id, rows))
        return result


if __name__ == '__main__':
    datasource = MockDataSource('t', 10, False)
    for data in datasource:
        print(data)

```

[查看源码](https://github.com/taosdata/TDengine/blob/main/docs/examples/python/mockdatasource.py)

</details>

<details>
<summary>run_write_task 函数</summary>

写进程每次从队列中取出尽量多的数据，并批量写入。

```python
def run_write_task(task_id: int, queue: Queue, done_queue: Queue):
    from sql_writer import SQLWriter
    log = logging.getLogger(f"WriteTask-{task_id}")
    writer = SQLWriter(get_connection)
    lines = None
    try:
        while True:
            over = False
            lines = []
            for _ in range(MAX_BATCH_SIZE):
                try:
                    line = queue.get_nowait()
                    if line == _DONE_MESSAGE:
                        over = True
                        break
                    if line:
                        lines.append(line)
                except Empty:
                    time.sleep(0.1)
            if len(lines) > 0:
                writer.process_lines(lines)
            if over:
                done_queue.put(_DONE_MESSAGE)
                break
    except KeyboardInterrupt:
        pass
    except BaseException as e:
        log.debug(f"lines={lines}")
        raise e
    finally:
        writer.close()
        log.debug('write task over')


```

[查看源码](https://github.com/taosdata/TDengine/blob/main/docs/examples/python/fast_write_example.py)

</details>

<details>

SQLWriter 类封装了拼 SQL 和写数据的逻辑。所有的表都没有提前创建，而是在发生表不存在错误的时候，再以超级表为模板批量建表，然后重新执行 INSERT 语句。对于其它错误会记录当时执行的 SQL， 以便排查错误和故障恢复。这个类也对 SQL 是否超过最大长度限制做了检查，根据 TDengine 3.0 的限制由输入参数 maxSQLLength 传入了支持的最大 SQL 长度，即 1,048,576 。

<summary>SQLWriter</summary>

```python
import logging
import taos


class SQLWriter:
    log = logging.getLogger("SQLWriter")

    def __init__(self, get_connection_func):
        self._tb_values = {}
        self._tb_tags = {}
        self._conn = get_connection_func()
        self._max_sql_length = self.get_max_sql_length()
        self._conn.execute("create database if not exists test keep 36500")
        self._conn.execute("USE test")

    def get_max_sql_length(self):
        rows = self._conn.query("SHOW variables").fetch_all()
        for r in rows:
            name = r[0]
            if name == "maxSQLLength":
                return int(r[1])
        return 1024 * 1024

    def process_lines(self, lines: [str]):
        """
        :param lines: [[tbName,ts,current,voltage,phase,location,groupId]]
        """
        for line in lines:
            ps = line.split(",")
            table_name = ps[0]
            value = '(' + ",".join(ps[1:-2]) + ') '
            if table_name in self._tb_values:
                self._tb_values[table_name] += value
            else:
                self._tb_values[table_name] = value

            if table_name not in self._tb_tags:
                location = ps[-2]
                group_id = ps[-1]
                tag_value = f"('{location}',{group_id})"
                self._tb_tags[table_name] = tag_value
        self.flush()

    def flush(self):
        """
        Assemble INSERT statement and execute it.
        When the sql length grows close to MAX_SQL_LENGTH, the sql will be executed immediately, and a new INSERT statement will be created.
        In case of "Table does not exit" exception, tables in the sql will be created and the sql will be re-executed.
        """
        sql = "INSERT INTO "
        sql_len = len(sql)
        buf = []
        for tb_name, values in self._tb_values.items():
            q = tb_name + " VALUES " + values
            if sql_len + len(q) >= self._max_sql_length:
                sql += " ".join(buf)
                self.execute_sql(sql)
                sql = "INSERT INTO "
                sql_len = len(sql)
                buf = []
            buf.append(q)
            sql_len += len(q)
        sql += " ".join(buf)
        self.create_tables()
        self.execute_sql(sql)
        self._tb_values.clear()

    def execute_sql(self, sql):
        try:
            self._conn.execute(sql)
        except taos.Error as e:
            error_code = e.errno & 0xffff
            # Table does not exit
            if error_code == 9731:
                self.create_tables()
            else:
                self.log.error("Execute SQL: %s", sql)
                raise e
        except BaseException as baseException:
            self.log.error("Execute SQL: %s", sql)
            raise baseException

    def create_tables(self):
        sql = "CREATE TABLE "
        for tb in self._tb_values.keys():
            tag_values = self._tb_tags[tb]
            sql += "IF NOT EXISTS " + tb + " USING meters TAGS " + tag_values + " "
        try:
            self._conn.execute(sql)
        except BaseException as e:
            self.log.error("Execute SQL: %s", sql)
            raise e

    def close(self):
        if self._conn:
            self._conn.close()


if __name__ == '__main__':
    def get_connection_func():
        conn = taos.connect()
        return conn


    writer = SQLWriter(get_connection_func=get_connection_func)
    writer.execute_sql(
        "create stable if not exists meters (ts timestamp, current float, voltage int, phase float) "
        "tags (location binary(64), groupId int)")
    writer.execute_sql(
        "INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) "
        "VALUES ('2021-07-13 14:06:32.272', 10.2, 219, 0.32)")

```

[查看源码](https://github.com/taosdata/TDengine/blob/main/docs/examples/python/sql_writer.py)

</details>

**执行步骤**

<details>

<summary>执行 Python 示例程序</summary>

1. 前提条件

   - 已安装 TDengine 客户端驱动
   - 已安装 Python3， 推荐版本 >= 3.8
   - 已安装 taospy

2. 安装 faster-fifo 代替 python 内置的 multiprocessing.Queue

   ```
   pip3 install faster-fifo
   ```

3. 点击上面的“查看源码”链接复制 `fast_write_example.py` 、 `sql_writer.py` 和 `mockdatasource.py` 三个文件。

4. 执行示例程序

   ```
   python3  fast_write_example.py <READ_TASK_COUNT> <WRITE_TASK_COUNT> <TABLE_COUNT> <QUEUE_SIZE> <MAX_BATCH_SIZE>
   ```

   下面是一次实际运行的输出, 机器配置 16 核 + 64G + 固态硬盘。

   ```
   root@vm85$ python3 fast_write_example.py  8 8
   2022-07-14 19:13:45,869 [root] - READ_TASK_COUNT=8, WRITE_TASK_COUNT=8, TABLE_COUNT=1000, QUEUE_SIZE=1000000, MAX_BATCH_SIZE=3000
   2022-07-14 19:13:48,882 [root] - WriteTask-0 started with pid 718347
   2022-07-14 19:13:48,883 [root] - WriteTask-1 started with pid 718348
   2022-07-14 19:13:48,884 [root] - WriteTask-2 started with pid 718349
   2022-07-14 19:13:48,884 [root] - WriteTask-3 started with pid 718350
   2022-07-14 19:13:48,885 [root] - WriteTask-4 started with pid 718351
   2022-07-14 19:13:48,885 [root] - WriteTask-5 started with pid 718352
   2022-07-14 19:13:48,886 [root] - WriteTask-6 started with pid 718353
   2022-07-14 19:13:48,886 [root] - WriteTask-7 started with pid 718354
   2022-07-14 19:13:48,887 [root] - ReadTask-0 started with pid 718355
   2022-07-14 19:13:48,888 [root] - ReadTask-1 started with pid 718356
   2022-07-14 19:13:48,889 [root] - ReadTask-2 started with pid 718357
   2022-07-14 19:13:48,889 [root] - ReadTask-3 started with pid 718358
   2022-07-14 19:13:48,890 [root] - ReadTask-4 started with pid 718359
   2022-07-14 19:13:48,891 [root] - ReadTask-5 started with pid 718361
   2022-07-14 19:13:48,892 [root] - ReadTask-6 started with pid 718364
   2022-07-14 19:13:48,893 [root] - ReadTask-7 started with pid 718365
   2022-07-14 19:13:56,042 [DataBaseMonitor] - count=6676310 speed=667631.0
   2022-07-14 19:14:06,196 [DataBaseMonitor] - count=20004310 speed=1332800.0
   2022-07-14 19:14:16,366 [DataBaseMonitor] - count=32290310 speed=1228600.0
   2022-07-14 19:14:26,527 [DataBaseMonitor] - count=44438310 speed=1214800.0
   2022-07-14 19:14:36,673 [DataBaseMonitor] - count=56608310 speed=1217000.0
   2022-07-14 19:14:46,834 [DataBaseMonitor] - count=68757310 speed=1214900.0
   2022-07-14 19:14:57,280 [DataBaseMonitor] - count=80992310 speed=1223500.0
   2022-07-14 19:15:07,689 [DataBaseMonitor] - count=93805310 speed=1281300.0
   2022-07-14 19:15:18,020 [DataBaseMonitor] - count=106111310 speed=1230600.0
   2022-07-14 19:15:28,356 [DataBaseMonitor] - count=118394310 speed=1228300.0
   2022-07-14 19:15:38,690 [DataBaseMonitor] - count=130742310 speed=1234800.0
   2022-07-14 19:15:49,000 [DataBaseMonitor] - count=143051310 speed=1230900.0
   2022-07-14 19:15:59,323 [DataBaseMonitor] - count=155276310 speed=1222500.0
   2022-07-14 19:16:09,649 [DataBaseMonitor] - count=167603310 speed=1232700.0
   2022-07-14 19:16:19,995 [DataBaseMonitor] - count=179976310 speed=1237300.0
   ```

</details>

:::note
使用 Python 连接器多进程连接 TDengine 的时候，有一个限制：不能在父进程中建立连接，所有连接只能在子进程中创建。
如果在父进程中创建连接，子进程再创建连接就会一直阻塞。这是个已知问题。

:::

</TabItem>
</Tabs>
