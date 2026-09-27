<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# KoP 缺失代码同步记录

对比日期：2026-09-27。

- 来源：本机 `kop`，`master@14f9832d8a437c5b717e3c74fd7e4cda9810aff7`。
- 目标基线：`starlight-for-kafka`，`4.0_ds@766d9891c97fb82024f0185628ca956315a570dc`。
- 目标继续使用 Pulsar **4.0.4**、Kafka **3.9.2**、`com.datastax.oss:4.0.3.9-SNAPSHOT`；未复制来源仓库的 POM、依赖升级及未提交修改。
- 目标原有 `.gitignore`、Lombok 版本、`TransactionTest` 和其他未跟踪文档保留。
- 经用户确认：补齐缺失模块和独立功能，保留当前 Broker 授权设计。

## 本次同步

1. **Kafka payload processor 三个 Maven 模块**：原始客户端实现、适配 Pulsar shaded client 的产物、实际使用 shaded 产物的测试。增加普通版测试及 Kafka Producer → Pulsar Consumer 集成测试。适配 Kafka 3.9.2 Compression API，修正上游多 batch 重复遍历及控制记录计数问题。
2. **OAuth 凭据 URL**：移植 `URL`、`PulsarURLStreamHandlerFactory`、`DataURLStreamHandler`，接入现有 `com.datastax.oss.kafka.oauth.ClientConfig`，支持本地路径与 data URL。保留当前延迟加载凭据、HTTP 客户端及 tenant token 处理方式。
3. **主题初始化失败**：移植 `KoPTopicInitializeException`，接入 `PartitionLog` 和 Produce 错误转换，使初始化失败返回 `NOT_LEADER_OR_FOLLOWER`。
4. **offset 写入超时**：移植 `offsetCommitTimeoutMs` 配置及发送超时接线，默认 5000 ms；SyncGroup 写入超时返回 `REBALANCE_IN_PROGRESS`，OffsetCommit 返回 `REQUEST_TIMED_OUT`。保留现有异步调度和其他错误映射。测试使用 BookKeeper 响应延迟注入。
5. **存储回归测试**：快照读写、重启恢复、序列化、Sarama v1 压缩记录计数，以及 precise publish throttling 覆盖；适配现有执行器和测试生命周期。

## 保留的差异

- `ClientInfo` 已存在于 `ClientCredentialsFlow.ClientInfo`，不再复制一个平行类。
- OAuth 独立 shaded 模块的职责已合并到当前 `oauth-client/pom.xml`；已有配置、凭据流测试在 DataStax 包下。
- KoP 的 SASL `groupId` 扩展、强制消费组校验、授权缓存，以及 `SlowAuthorizationTest`、`SaslOAuthKopHandlersWithGroupIdTest` 不在本次同步范围。`KafkaAuthorizationMockTestBase` 是对应授权测试重构的一部分。
- 大量 tests 文件只调整了包和目录；同名匹配用于发现重构，不能推导两边全部逻辑相同。
- 未覆盖当前已有类的所有实现差异，未覆盖上游 README、部署示例和发布工作流。

## 使用 payload processor

常规 Pulsar shaded client 使用 Maven 产物 `com.datastax.oss:kafka-payload-processor:4.0.3.9-SNAPSHOT`；使用 `pulsar-client-original` 时选择 `kafka-payload-processor-original`。这是客户端组件，不需要加入 Broker NAR。

```java
client.newConsumer(Schema.STRING)
    .topic("my-topic")
    .subscriptionName("my-subscription")
    .messagePayloadProcessor(new KafkaPayloadProcessor())
    .subscribe();
```

## 验证

使用本机 **JDK 17**，不同定向运行共覆盖 **80 个测试用例**，下列各测试类最近一次运行均无失败、错误或跳过；未执行全量项目测试。

| 验证范围 | 通过用例 |
| --- | ---: |
| 普通 payload processor | 9 |
| shaded payload processor（含 relocation 断言） | 10 |
| OAuth 配置与凭据读取 | 8 |
| Kafka Producer → Pulsar Consumer | 1 |
| precise publish throttling | 4 |
| offset 写入超时 | 2 |
| PartitionLog（含 Sarama 和初始化失败） | 13 |
| 单分区/分区快照 | 5 |
| 现有 GroupMetadataManager 回归 | 28 |

主要命令（在仓库根目录执行；JDK 17 路径可按本机调整）：

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home
mvn -B -ntp -pl kafka-payload-processor-shaded-tests,oauth-client -am verify \
  -Dtest=KafkaPayloadProcessorTest,ClientConfigTest,ClientCredentialsFlowTest \
  -Dsurefire.failIfNoSpecifiedTests=false
mvn -B -ntp -pl tests -am test \
  -Dtest=PartitionLogTest,OffsetTopicWriteTimeoutTest,GroupMetadataManagerTest,KafkaPayloadProcessorIntegrationTest,PulsarTopicProducerStateManagerSnapshotBufferTest,PulsarPartitionedTopicProducerStateManagerSnapshotBufferTest,PreciselyMessagePublishThrottlingTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

本机存储测试实际使用了 `-o -s /tmp/kop-sync-20260927/settings.xml`：该临时配置注册了缓存来源 `jitpack.io`，使 Maven 能读取已经缓存的 Everit 依赖；未改动用户 Maven 配置或项目仓库设置。默认在线命令曾遇到 DataStax 仓库 401。默认 JDK 21 的旧 Byte Buddy 不兼容问题通过 JDK 17 解决。

- `checkstyle:check`：本次涉及模块及其 reactor 依赖检查通过，命令排除了 `**/TransactionTest.java`。不带排除项时，当前用户原有 `TransactionTest.java:70` 的 unused import 导致失败，原文件保持不变。
- `license:check`：将本次 39 个文件复制到临时校验目录后执行原项目许可证规则，通过；未宣称整个现有工作区的许可证检查通过。
- `git diff --check`：通过。
- Sarama 回归先出现 GZIP/SNAPPY/LZ4 三种压缩场景 `expected 3 but found 1`，同步对应生产修复后全部通过。
- 运行日志位于 `/tmp/kop-sync-20260927/`；Surefire XML 位于对应模块 `target/surefire-reports/`。

新增 offset timeout 默认值为 **5000 ms**；它只表示请求等待超时，不保证撤销已经提交的底层写入。

## 98 个源路径差异的逐项处理

| KoP 路径 | 处理 | 目标路径 / 对应实现 |
| --- | --- | --- |
| `.github/CODEOWNERS` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `.github/PULL_REQUEST_TEMPLATE.md` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `.github/workflows/release.yaml` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `docker-compose-cluster.yaml` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `docs/configuration.md` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `docs/cve_list.md` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `docs/implementation.md` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `docs/kop-architecture.png` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `docs/kop.md` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `docs/kop.yaml` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `docs/reference-metrics.md` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `docs/release-process.md` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `docs/schema.md` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `docs/security.md` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `docs/upgrade.md` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `kafka-impl/src/assemble/bin.xml` | 非缺失功能源码；保留当前仓库文档/发布/部署配置 | `—` |
| `kafka-impl/src/main/java/io/streamnative/pulsar/handlers/kop/exceptions/KoPTopicInitializeException.java` | 已同步 | `kafka-impl/src/main/java/io/streamnative/pulsar/handlers/kop/exceptions/KoPTopicInitializeException.java` |
| `kafka-impl/src/test/java/io/streamnative/pulsar/handlers/kop/security/oauth/ServerConfigTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `kafka-impl/src/test/java/com/datastax/oss/kafka/oauth/ServerConfigTest.java` |
| `kafka-payload-processor-shaded-tests/pom.xml` | 已同步 | `kafka-payload-processor-shaded-tests/pom.xml` |
| `kafka-payload-processor-shaded-tests/src/test/java/io/streamnative/pulsar/handlers/kop/ByteBufferPayload.java` | 已同步 | `kafka-payload-processor-shaded-tests/src/test/java/io/streamnative/pulsar/handlers/kop/ByteBufferPayload.java` |
| `kafka-payload-processor-shaded-tests/src/test/java/io/streamnative/pulsar/handlers/kop/KafkaPayloadProcessorTest.java` | 已同步 | `kafka-payload-processor-shaded-tests/src/test/java/io/streamnative/pulsar/handlers/kop/KafkaPayloadProcessorTest.java` |
| `kafka-payload-processor-shaded-tests/src/test/java/io/streamnative/pulsar/handlers/kop/MockedMessagePayloadContext.java` | 已同步 | `kafka-payload-processor-shaded-tests/src/test/java/io/streamnative/pulsar/handlers/kop/MockedMessagePayloadContext.java` |
| `kafka-payload-processor-shaded/pom.xml` | 已同步 | `kafka-payload-processor-shaded/pom.xml` |
| `kafka-payload-processor/pom.xml` | 已同步 | `kafka-payload-processor/pom.xml` |
| `kafka-payload-processor/src/main/java/io/streamnative/pulsar/handlers/kop/KafkaPayloadProcessor.java` | 已同步 | `kafka-payload-processor/src/main/java/io/streamnative/pulsar/handlers/kop/KafkaPayloadProcessor.java` |
| `kafka-payload-processor/src/main/java/io/streamnative/pulsar/handlers/kop/package-info.java` | 已同步 | `kafka-payload-processor/src/main/java/io/streamnative/pulsar/handlers/kop/package-info.java` |
| `oauth-client-shaded-test/pom.xml` | 已有打包职责；保留现有单模块 shade 方式 | `oauth-client/pom.xml` |
| `oauth-client-shaded-test/src/test/java/io/streamnative/pulsar/handlers/kop/security/oauth/ClientConfigHelper.java` | 已有测试，包和模块不同 | `oauth-client/src/test/java/com/datastax/oss/kafka/oauth/ClientConfigHelper.java` |
| `oauth-client-shaded-test/src/test/java/io/streamnative/pulsar/handlers/kop/security/oauth/ClientConfigTest.java` | 已有测试，包和模块不同 | `oauth-client/src/test/java/com/datastax/oss/kafka/oauth/ClientConfigTest.java` |
| `oauth-client-shaded-test/src/test/java/io/streamnative/pulsar/handlers/kop/security/oauth/ClientCredentialsFlowTest.java` | 已有测试，包和模块不同 | `oauth-client/src/test/java/com/datastax/oss/kafka/oauth/ClientCredentialsFlowTest.java` |
| `oauth-client-shaded-test/src/test/java/io/streamnative/pulsar/handlers/kop/security/oauth/ShadedTest.java` | 已有打包职责；保留现有单模块 shade 方式 | `oauth-client/pom.xml` |
| `oauth-client-shaded-test/src/test/resources/private_key.json` | 已有同名测试资源 | `oauth-client/src/test/resources/private_key.json` |
| `oauth-client-shaded-test/src/test/resources/private_key_with_group_id.json` | 按确认范围保留差异：授权设计 | `—` |
| `oauth-client-shaded/pom.xml` | 已有打包职责；保留现有单模块 shade 方式 | `oauth-client/pom.xml` |
| `oauth-client/src/main/java/io/streamnative/pulsar/handlers/kop/security/oauth/ClientConfig.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `oauth-client/src/main/java/com/datastax/oss/kafka/oauth/ClientConfig.java` |
| `oauth-client/src/main/java/io/streamnative/pulsar/handlers/kop/security/oauth/ClientCredentialsFlow.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `oauth-client/src/main/java/com/datastax/oss/kafka/oauth/ClientCredentialsFlow.java` |
| `oauth-client/src/main/java/io/streamnative/pulsar/handlers/kop/security/oauth/ClientInfo.java` | 已有内嵌类；group_id 扩展按确认范围不迁移 | `oauth-client/src/main/java/com/datastax/oss/kafka/oauth/ClientCredentialsFlow.java` |
| `oauth-client/src/main/java/io/streamnative/pulsar/handlers/kop/security/oauth/OAuthBearerTokenImpl.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `oauth-client/src/main/java/com/datastax/oss/kafka/oauth/OAuthBearerTokenImpl.java` |
| `oauth-client/src/main/java/io/streamnative/pulsar/handlers/kop/security/oauth/OauthLoginCallbackHandler.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `oauth-client/src/main/java/com/datastax/oss/kafka/oauth/OauthLoginCallbackHandler.java` |
| `oauth-client/src/main/java/io/streamnative/pulsar/handlers/kop/security/oauth/package-info.java` | 已有对应包说明 | `oauth-client/src/main/java/com/datastax/oss/kafka/oauth/package-info.java` |
| `oauth-client/src/main/java/io/streamnative/pulsar/handlers/kop/security/oauth/url/DataURLStreamHandler.java` | 已同步并适配包名 | `oauth-client/src/main/java/com/datastax/oss/kafka/oauth/url/DataURLStreamHandler.java` |
| `oauth-client/src/main/java/io/streamnative/pulsar/handlers/kop/security/oauth/url/PulsarURLStreamHandlerFactory.java` | 已同步并适配包名 | `oauth-client/src/main/java/com/datastax/oss/kafka/oauth/url/PulsarURLStreamHandlerFactory.java` |
| `oauth-client/src/main/java/io/streamnative/pulsar/handlers/kop/security/oauth/url/URL.java` | 已同步并适配包名 | `oauth-client/src/main/java/com/datastax/oss/kafka/oauth/url/URL.java` |
| `oauth-client/src/main/java/io/streamnative/pulsar/handlers/kop/security/oauth/url/package-info.java` | 已同步并适配包名 | `oauth-client/src/main/java/com/datastax/oss/kafka/oauth/url/package-info.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/OffsetTopicWriteTimeoutTest.java` | 已同步 | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/OffsetTopicWriteTimeoutTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/admin/KafkaAdminTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/KafkaAdminTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/admin/KopBrokerLookupManagerTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/KopBrokerLookupManagerTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/admin/OffsetResetTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/OffsetResetTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/coordinator/transaction/TransactionTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/TransactionTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/coordinator/transaction/TransactionWithOAuthBearerAuthTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/TransactionWithOAuthBearerAuthTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/e2e/BasicEndToEndKafkaTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/BasicEndToEndKafkaTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/e2e/BasicEndToEndPulsarTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/BasicEndToEndPulsarTest.java, tests/src/test/java/io/streamnative/pulsar/handlers/kop/compatibility/BasicEndToEndPulsarTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/e2e/BasicEndToEndTestBase.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/BasicEndToEndTestBase.java, tests/src/test/java/io/streamnative/pulsar/handlers/kop/compatibility/BasicEndToEndTestBase.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/e2e/DistributedClusterTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/DistributedClusterTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/e2e/KafkaMessageOrderKafkaTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/KafkaMessageOrderKafkaTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/e2e/KafkaMessageOrderPulsarTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/KafkaMessageOrderPulsarTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/e2e/KafkaMessageOrderTestBase.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/KafkaMessageOrderTestBase.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/e2e/KafkaNonPartitionedTopicTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/KafkaNonPartitionedTopicTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/e2e/MultiLedgerTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/MultiLedgerTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/e2e/PerTopicConfigurationTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/PerTopicConfigurationTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/metadata/DifferentNamespaceTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/DifferentNamespaceTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/metadata/MetadataInitTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/MetadataInitTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/metrics/KafkaProducerStatsTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/KafkaProducerStatsTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/metrics/MetricsProviderTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/MetricsProviderTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/metrics/MetricsProviderWithDisableGroupLevelConsumerMetricsTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/MetricsProviderWithDisableGroupLevelConsumerMetricsTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/producer/IdempotentProducerTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/IdempotentProducerTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/producer/InnerTopicProtectionTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/InnerTopicProtectionTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/producer/MessagePublishBufferThrottleKafkaTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/MessagePublishBufferThrottleKafkaTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/producer/MessagePublishBufferThrottlePulsarTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/MessagePublishBufferThrottlePulsarTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/producer/MessagePublishBufferThrottleTestBase.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/MessagePublishBufferThrottleTestBase.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/producer/MessagePublishThrottlingTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/MessagePublishThrottlingTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/producer/PreciselyMessagePublishThrottlingTest.java` | 已同步并适配现有测试基类 | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/PreciselyMessagePublishThrottlingTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/producer/PublishRateLimitTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/PublishRateLimitTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/schemaregistry/model/impl/SchemaRegistryTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/SchemaRegistryTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/schemaregistry/model/impl/SchemaRestApiTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/SchemaRestApiTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/KafkaSSLChannelTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/KafkaSSLChannelTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/KafkaSSLChannelWithClientAuthTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/KafkaSSLChannelWithClientAuthTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/SaslPlainKafkaTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/SaslPlainKafkaTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/SaslPlainPulsarTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/SaslPlainPulsarTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/SaslPlainTestBase.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/SaslPlainTestBase.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/auth/DelayAuthorizationFailedCloseTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/DelayAuthorizationFailedCloseTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/auth/KafkaAuthorizationKafkaMultitenantTenantMetadataTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/KafkaAuthorizationKafkaMultitenantTenantMetadataTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/auth/KafkaAuthorizationMockTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/KafkaAuthorizationMockTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/auth/KafkaAuthorizationMockTestBase.java` | 按确认范围保留差异：授权设计 | `—` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/auth/KafkaAuthorizationPulsarTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/KafkaAuthorizationPulsarTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/auth/KafkaAuthorizationTestBase.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/KafkaAuthorizationTestBase.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/auth/SlowAuthorizationTest.java` | 按确认范围保留差异：授权设计 | `—` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/oauth/CustomOAuthBearerCallbackHandlerTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/CustomOAuthBearerCallbackHandlerTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/oauth/HydraOAuthUtils.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/HydraOAuthUtils.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/oauth/SaslOAuthBearerTestBase.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/SaslOAuthBearerTestBase.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/oauth/SaslOAuthDefaultHandlersTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/SaslOAuthDefaultHandlersTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/oauth/SaslOAuthKopHandlersTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/SaslOAuthKopHandlersTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/oauth/SaslOAuthKopHandlersWithGroupIdTest.java` | 按确认范围保留差异：授权设计 | `—` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/security/oauth/SaslOAuthKopHandlersWithMultiTenantTest.java` | 已有同名类，路径/包不同（未宣称实现完全等同） | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/SaslOAuthKopHandlersWithMultiTenantTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/storage/ProducerStateManagerSnapshotBufferTestBase.java` | 已同步 | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/storage/ProducerStateManagerSnapshotBufferTestBase.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/storage/PulsarPartitionedTopicProducerStateManagerSnapshotBufferTest.java` | 已同步 | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/storage/PulsarPartitionedTopicProducerStateManagerSnapshotBufferTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/storage/PulsarTopicProducerStateManagerSnapshotBufferTest.java` | 已同步 | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/storage/PulsarTopicProducerStateManagerSnapshotBufferTest.java` |
| `tests/src/test/java/io/streamnative/pulsar/handlers/kop/storage/SaramaCompressedV1Records.java` | 已同步 | `tests/src/test/java/io/streamnative/pulsar/handlers/kop/storage/SaramaCompressedV1Records.java` |
