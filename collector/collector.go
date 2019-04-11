package collector

import (
	"github.com/google-cloud-tools/kafka-minion/options"
	"github.com/google-cloud-tools/kafka-minion/storage"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"strconv"
)

var (
	// Consumer group metrics
	groupPartitionOffsetDesc      *prometheus.Desc
	groupPartitionCommitCountDesc *prometheus.Desc
	groupPartitionLastCommitDesc  *prometheus.Desc
	groupPartitionLagDesc         *prometheus.Desc
	groupTopicLagDesc             *prometheus.Desc

	// Topic metrics
	partitionCountDesc *prometheus.Desc

	// Partition metrics
	partitionLowWaterMarkDesc  *prometheus.Desc
	partitionHighWaterMarkDesc *prometheus.Desc
	partitionMessageCountDesc  *prometheus.Desc

	// misc kafka_exporter metrics
	brokerCountDesc              *prometheus.Desc
	partitionInSyncReplicasDesc  *prometheus.Desc
	partitionLeaderDesc          *prometheus.Desc
	partitionLeaderPreferredDesc *prometheus.Desc
	partitionReplicasDesc        *prometheus.Desc
	partitionUnderReplicatedDesc *prometheus.Desc
)

// Collector collects and provides all Kafka metrics on each /metrics invocation, see:
// https://godoc.org/github.com/prometheus/client_golang/prometheus#hdr-Custom_Collectors_and_constant_Metrics
type Collector struct {
	opts    *options.Options
	storage *storage.OffsetStorage
	logger  *log.Entry
}

// versionedConsumerGroup represents the information which one could interpret by looking at all consumer group names
// For instance consumer group name "sample-group-1" has base name "sample-group-", version: 1 and is the latest as long
// as there is no group with the same base name and a higher appending number than 1
type versionedConsumerGroup struct {
	BaseName string
	Name     string
	Version  uint32
	IsLatest bool
}

// NewCollector returns a new prometheus collector, preinitialized with all the to be exposed metrics under respect
// of the metrics prefix which can be passed via environment variables
func NewCollector(opts *options.Options, storage *storage.OffsetStorage) *Collector {
	logger := log.WithFields(log.Fields{
		"module": "collector",
	})

	if opts.KafkaExporterCompat {
		// Consumer group metrics
		groupPartitionOffsetDesc = prometheus.NewDesc(
			"kafka_consumergroup_current_offset",
			"Newest commited offset of a consumer group for a partition",
			[]string{"consumergroup", "group_base_name", "group_is_latest", "group_version", "topic", "partition"}, prometheus.Labels{},
		)

		groupPartitionCommitCountDesc = prometheus.NewDesc(
			"kafka_consumergroup_commit_count",
			"Number of commits of a consumer group for a partition",
			[]string{"consumergroup", "group_base_name", "group_is_latest", "group_version", "topic", "partition"}, prometheus.Labels{},
		)

		groupPartitionLastCommitDesc = prometheus.NewDesc(
			"kafka_consumergroup_last_commit",
			"Timestamp when consumer group last commited an offset for a partition",
			[]string{"consumergroup", "group_base_name", "group_is_latest", "group_version", "topic", "partition"}, prometheus.Labels{},
		)

		groupPartitionLagDesc = prometheus.NewDesc(
			"kafka_consumergroup_lag",
			"Number of messages the consumer group is behind for a partition",
			[]string{"consumergroup", "group_base_name", "group_is_latest", "group_version", "topic", "partition"}, prometheus.Labels{},
		)

		groupTopicLagDesc = prometheus.NewDesc(
			"kafka_consumergroup_lag_sum",
			"Number of messages the consumer group is behind for a topic",
			[]string{"consumergroup", "group_base_name", "group_is_latest", "group_version", "topic"}, prometheus.Labels{},
		)

		// Topic metrics
		partitionCountDesc = prometheus.NewDesc(
			"kafka_topic_partitions",
			"Partition count for a given topic along with cleanup policy as label",
			[]string{"topic", "cleanup_policy"}, prometheus.Labels{},
		)

		// Partition metrics
		partitionHighWaterMarkDesc = prometheus.NewDesc(
			"kafka_topic_partition_current_offset",
			"Highest known commited offset for this partition",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)
		partitionLowWaterMarkDesc = prometheus.NewDesc(
			"kafka_topic_partition_oldest_offset",
			"Oldest known commited offset for this partition",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)
		partitionMessageCountDesc = prometheus.NewDesc(
			"kafka_topic_partition_message_count",
			"Number of messages for a given topic. Calculated by subtracting high water mark by low water mark.",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)

		partitionMessageCountDesc = prometheus.NewDesc(
			"kafka_topic_partition_message_count",
			"Number of messages for a given topic. Calculated by subtracting high water mark by low water mark.",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)

		// new

		brokerCountDesc = prometheus.NewDesc(
			"kafka_brokers",
			"Number of kafka brokers in cluster.",
			[]string{}, prometheus.Labels{},
		)

		partitionInSyncReplicasDesc = prometheus.NewDesc(
			"kafka_topic_partition_in_sync_replica",
			"Number of in-sync replicas for this partition.",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)

		partitionLeaderDesc = prometheus.NewDesc(
			"kafka_topic_partition_leader",
			"Leader broker ID for this partition.",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)

		partitionLeaderPreferredDesc = prometheus.NewDesc(
			"kafka_topic_partition_leader_is_preferred",
			"Whether the leader broker for this partition is the preferred broker.",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)

		partitionReplicasDesc = prometheus.NewDesc(
			"kafka_topic_partition_replicas",
			"Number of replicas for this partition.",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)

		partitionUnderReplicatedDesc = prometheus.NewDesc(
			"kafka_topic_partition_under_replicated_partition",
			"Whether this partition is under-replicated.",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)

	} else {
		// Consumer group metrics
		groupPartitionOffsetDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "group_topic_partition", "offset"),
			"Newest commited offset of a consumer group for a partition",
			[]string{"group", "group_base_name", "group_is_latest", "group_version", "topic", "partition"}, prometheus.Labels{},
		)
		groupPartitionCommitCountDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "group_topic_partition", "commit_count"),
			"Number of commits of a consumer group for a partition",
			[]string{"group", "group_base_name", "group_is_latest", "group_version", "topic", "partition"}, prometheus.Labels{},
		)
		groupPartitionLastCommitDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "group_topic_partition", "last_commit"),
			"Timestamp when consumer group last commited an offset for a partition",
			[]string{"group", "group_base_name", "group_is_latest", "group_version", "topic", "partition"}, prometheus.Labels{},
		)
		groupPartitionLagDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "group_topic_partition", "lag"),
			"Number of messages the consumer group is behind for a partition",
			[]string{"group", "group_base_name", "group_is_latest", "group_version", "topic", "partition"}, prometheus.Labels{},
		)
		groupTopicLagDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "group_topic", "lag"),
			"Number of messages the consumer group is behind for a topic",
			[]string{"group", "group_base_name", "group_is_latest", "group_version", "topic"}, prometheus.Labels{},
		)

		// Topic metrics
		partitionCountDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "topic", "partition_count"),
			"Partition count for a given topic along with cleanup policy as label",
			[]string{"topic", "cleanup_policy"}, prometheus.Labels{},
		)

		// Partition metrics
		partitionHighWaterMarkDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "topic_partition", "high_water_mark"),
			"Highest known commited offset for this partition",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)
		partitionLowWaterMarkDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "topic_partition", "low_water_mark"),
			"Oldest known commited offset for this partition",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)
		partitionMessageCountDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "topic_partition", "message_count"),
			"Number of messages for a given topic. Calculated by subtracting high water mark by low water mark.",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)

		partitionMessageCountDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "topic_partition", "message_count"),
			"Number of messages for a given topic. Calculated by subtracting high water mark by low water mark.",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)

		brokerCountDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "cluster", "broker_count"),
			"Number of kafka brokers in cluster.",
			[]string{}, prometheus.Labels{},
		)

		partitionInSyncReplicasDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "topic_partition", "isr_count"),
			"Number of in-sync replicas for this partition.",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)

		partitionLeaderDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "topic_partition", "leader"),
			"Leader broker ID for this partition.",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)

		partitionLeaderPreferredDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "topic_partition", "leader_preferred"),
			"Whether the leader broker for this partition is the preferred broker.",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)

		partitionReplicasDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "topic_partition", "replica_count"),
			"Number of replicas for this partition.",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)

		partitionUnderReplicatedDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.MetricsPrefix, "topic_partition", "under_replicated"),
			"Whether this partition is under-replicated.",
			[]string{"topic", "partition"}, prometheus.Labels{},
		)
	}

	return &Collector{
		opts,
		storage,
		logger,
	}
}

// Describe sends a description of all to be exposed metric types to Prometheus
func (e *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- groupPartitionOffsetDesc
}

// Collect is triggered by the Prometheus registry when the metrics endpoint has been invoked
func (e *Collector) Collect(ch chan<- prometheus.Metric) {
	log.Debug("Collector's collect has been invoked")

	if e.storage.IsConsumed() == false {
		log.Info("Offets topic has not yet been consumed until the end")
		return
	}

	consumerOffsets := e.storage.ConsumerOffsets()
	partitionLowWaterMarks := e.storage.PartitionLowWaterMarks()
	partitionHighWaterMarks := e.storage.PartitionHighWaterMarks()
	partitionStatuses := e.storage.PartitionStatuses()
	topicConfigs := e.storage.TopicConfigs()
	clusterInfo := e.storage.ClusterInfo()

	e.collectConsumerOffsets(ch, consumerOffsets, partitionLowWaterMarks, partitionHighWaterMarks)

	ch <- prometheus.MustNewConstMetric(
		brokerCountDesc,
		prometheus.GaugeValue,
		float64(clusterInfo.BrokerCount),
	)

	for _, config := range topicConfigs {
		ch <- prometheus.MustNewConstMetric(
			partitionCountDesc,
			prometheus.GaugeValue,
			float64(config.PartitionCount),
			config.TopicName,
			config.CleanupPolicy,
		)
	}

	for _, partitions := range partitionLowWaterMarks {
		for _, partition := range partitions {
			ch <- prometheus.MustNewConstMetric(
				partitionLowWaterMarkDesc,
				prometheus.GaugeValue,
				float64(partition.WaterMark),
				partition.TopicName,
				strconv.Itoa(int(partition.PartitionID)),
			)
		}
	}

	for _, partitions := range partitionHighWaterMarks {
		for _, partition := range partitions {
			ch <- prometheus.MustNewConstMetric(
				partitionHighWaterMarkDesc,
				prometheus.GaugeValue,
				float64(partition.WaterMark),
				partition.TopicName,
				strconv.Itoa(int(partition.PartitionID)),
			)
		}
	}

	for _, partitions := range partitionHighWaterMarks {
		for _, partition := range partitions {
			topicName := partition.TopicName
			partitionID := partition.PartitionID
			if lowWaterMark, exists := partitionLowWaterMarks[topicName][partitionID]; exists {
				ch <- prometheus.MustNewConstMetric(
					partitionMessageCountDesc,
					prometheus.GaugeValue,
					float64(partition.WaterMark-lowWaterMark.WaterMark),
					partition.TopicName,
					strconv.Itoa(int(partition.PartitionID)),
				)
			}
		}
	}

	// metrics from partition statuses
	for _, partitions := range partitionStatuses {
		for _, partition := range partitions {
			ch <- prometheus.MustNewConstMetric(
				partitionInSyncReplicasDesc,
				prometheus.GaugeValue,
				float64(partition.InSyncReplicas),
				partition.TopicName,
				strconv.Itoa(int(partition.PartitionID)),
			)

			ch <- prometheus.MustNewConstMetric(
				partitionReplicasDesc,
				prometheus.GaugeValue,
				float64(len(partition.Replicas)),
				partition.TopicName,
				strconv.Itoa(int(partition.PartitionID)),
			)

			ch <- prometheus.MustNewConstMetric(
				partitionLeaderDesc,
				prometheus.GaugeValue,
				float64(partition.Leader),
				partition.TopicName,
				strconv.Itoa(int(partition.PartitionID)),
			)

			ch <- prometheus.MustNewConstMetric(
				partitionLeaderPreferredDesc,
				prometheus.GaugeValue,
				bool2float(partition.LeaderIsPreferred),
				partition.TopicName,
				strconv.Itoa(int(partition.PartitionID)),
			)

			ch <- prometheus.MustNewConstMetric(
				partitionUnderReplicatedDesc,
				prometheus.GaugeValue,
				bool2float(partition.UnderReplicated),
				partition.TopicName,
				strconv.Itoa(int(partition.PartitionID)),
			)
		}
	}
}

func bool2float(val bool) float64 {
	if val {
		return 1.0
	} else {
		return 0.0
	}
}

type groupLag struct {
	versionedGroup *versionedConsumerGroup
	lagByTopic     map[string]int64
}

func (e *Collector) collectConsumerOffsets(ch chan<- prometheus.Metric, offsets map[string]storage.ConsumerPartitionOffsetMetric,
	lowWaterMarks map[string]storage.PartitionWaterMarks, highWaterMarks map[string]storage.PartitionWaterMarks) {
	consumerGroups := getVersionedConsumerGroups(offsets)

	errorTopics := make(map[string]bool)
	groupLagsByGroupName := make(map[string]groupLag)

	// Partition offsets and lags
	for _, offset := range offsets {
		group := consumerGroups[offset.Group]
		// Offset metric
		ch <- prometheus.MustNewConstMetric(
			groupPartitionOffsetDesc,
			prometheus.GaugeValue,
			float64(offset.Offset),
			offset.Group,
			group.BaseName,
			strconv.FormatBool(group.IsLatest),
			strconv.Itoa(int(group.Version)),
			offset.Topic,
			strconv.Itoa(int(offset.Partition)),
		)

		// Commit count metric
		ch <- prometheus.MustNewConstMetric(
			groupPartitionCommitCountDesc,
			prometheus.CounterValue,
			offset.TotalCommitCount,
			offset.Group,
			group.BaseName,
			strconv.FormatBool(group.IsLatest),
			strconv.Itoa(int(group.Version)),
			offset.Topic,
			strconv.Itoa(int(offset.Partition)),
		)

		// Last commit metric
		ch <- prometheus.MustNewConstMetric(
			groupPartitionLastCommitDesc,
			prometheus.GaugeValue,
			float64(offset.Timestamp),
			offset.Group,
			group.BaseName,
			strconv.FormatBool(group.IsLatest),
			strconv.Itoa(int(group.Version)),
			offset.Topic,
			strconv.Itoa(int(offset.Partition)),
		)

		if _, exists := lowWaterMarks[offset.Topic][offset.Partition]; !exists {
			errorTopics[offset.Topic] = true
			e.logger.WithFields(log.Fields{
				"topic":     offset.Topic,
				"partition": offset.Partition,
			}).Warn("could not calculate partition lag because low water mark is missing")
			continue
		}
		partitionLowWaterMark := lowWaterMarks[offset.Topic][offset.Partition].WaterMark
		if _, exists := highWaterMarks[offset.Topic][offset.Partition]; !exists {
			errorTopics[offset.Topic] = true
			e.logger.WithFields(log.Fields{
				"topic":     offset.Topic,
				"partition": offset.Partition,
			}).Warn("could not calculate partition lag because high water mark is missing")
			continue
		}
		partitionHighWaterMark := highWaterMarks[offset.Topic][offset.Partition].WaterMark

		var lag int64
		if offset.Offset > partitionHighWaterMark {
			// Partition offsets are updated periodically, while consumer offsets continously flow in. Hence it's possible
			// that consumer offset might be ahead of the partition high watermark. For this case mark it as zero lag
			lag = 0
		} else if offset.Offset < partitionLowWaterMark {
			// If last commited offset does not exist anymore due to delete policy (e. g. 1day retention, 3day old commit)
			lag = partitionHighWaterMark - partitionLowWaterMark
		} else {
			lag = partitionHighWaterMark - offset.Offset
		}

		// Add partition lag to group:topic lag aggregation
		if _, exists := groupLagsByGroupName[offset.Group]; !exists {
			groupLagsByGroupName[offset.Group] = groupLag{
				versionedGroup: group,
				lagByTopic:     make(map[string]int64),
			}
		}
		groupLagsByGroupName[offset.Group].lagByTopic[offset.Topic] += lag

		ch <- prometheus.MustNewConstMetric(
			groupPartitionLagDesc,
			prometheus.GaugeValue,
			float64(lag),
			offset.Group,
			group.BaseName,
			strconv.FormatBool(group.IsLatest),
			strconv.Itoa(int(group.Version)),
			offset.Topic,
			strconv.Itoa(int(offset.Partition)),
		)
	}

	// Group lags
	for groupName, groupLag := range groupLagsByGroupName {
		for topicName, topicLag := range groupLag.lagByTopic {
			if _, hasErrors := errorTopics[topicName]; hasErrors {
				e.logger.WithFields(log.Fields{
					"group": groupName,
					"topic": topicName,
				}).Warn("cannot calculate group lag due to a missing partition watermark")
				continue
			}
			ch <- prometheus.MustNewConstMetric(
				groupTopicLagDesc,
				prometheus.GaugeValue,
				float64(topicLag),
				groupLag.versionedGroup.Name,
				groupLag.versionedGroup.BaseName,
				strconv.FormatBool(groupLag.versionedGroup.IsLatest),
				strconv.Itoa(int(groupLag.versionedGroup.Version)),
				topicName,
			)
		}
	}
}

func getVersionedConsumerGroups(offsets map[string]storage.ConsumerPartitionOffsetMetric) map[string]*versionedConsumerGroup {
	// This map contains all known consumer groups. Key is the full group name
	groupsByName := make(map[string]*versionedConsumerGroup)

	// This map is supposed to contain only the highest versioned consumer within a consumer group base name
	latestGroupByBaseName := make(map[string]*versionedConsumerGroup)
	for _, offset := range offsets {
		consumerGroup := parseConsumerGroupName(offset.Group)
		groupsByName[offset.Group] = consumerGroup
		baseName := consumerGroup.BaseName
		if _, ok := latestGroupByBaseName[baseName]; ok {
			// Overwrite entry for this base name if consumergroup version is higher
			if latestGroupByBaseName[baseName].Version < consumerGroup.Version {
				latestGroupByBaseName[baseName] = consumerGroup
			}
		} else {
			latestGroupByBaseName[baseName] = consumerGroup
		}
	}

	// Set IsLatest if this consumer group is the highest known version within this group base name
	for _, group := range latestGroupByBaseName {
		groupsByName[group.Name].IsLatest = true
	}

	return groupsByName
}

// parseConsumerGroupName returns the "base name" of a consumer group and it's version
// Given the name "sample-group-01" the base name would be "sample-group" and the version is "1"
// If there's no appending number it's being considered as version 0
func parseConsumerGroupName(consumerGroupName string) *versionedConsumerGroup {
	parsedVersion, baseName := parseVersion(consumerGroupName, "", len(consumerGroupName)-1)
	return &versionedConsumerGroup{BaseName: baseName, Name: consumerGroupName, Version: uint32(parsedVersion), IsLatest: false}
}

// parseVersion tries to parse a "version" from a consumer group name. An appending number of a
// consumer group name is considered as it's version. It returns the parsed version and the consumer group base name.
func parseVersion(groupName string, versionString string, digitIndexCursor int) (uint32, string) {
	if len(groupName) == 0 {
		return 0, ""
	}

	// Try to parse a digit from right to left, so that we correctly identify names like "consumer-group-v003" as well
	lastCharacter := groupName[digitIndexCursor : digitIndexCursor+1]
	_, err := strconv.Atoi(lastCharacter)
	if err != nil {
		if len(versionString) == 0 {
			return 0, groupName[0 : digitIndexCursor+1]
		}

		// We've got a versionString, but this character is no digit anymore
		version, err := strconv.ParseUint(versionString, 10, 0)
		if err != nil {
			// should never happen, because version string must only consist of valid ints
			return 0, groupName[0:digitIndexCursor]
		}
		return uint32(version), groupName[0 : digitIndexCursor+1]
	}

	// Last character is a valid digit, so we can prepend it to the "versionString" which we can try to
	// parse as int when we are done
	newVersionedString := lastCharacter + versionString
	indexCursor := digitIndexCursor - 1

	return parseVersion(groupName, newVersionedString, indexCursor)
}
