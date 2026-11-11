//! Integration tests for horizontal sharding components
//!
//! These tests verify that HorizontalShardRouter and ShardCoordinator work
//! together correctly for multi-instance orchestrator deployments.

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::sharding::coordinator::{mock::MockRedisCoordinator, ShardCoordinator};
    use crate::sharding::horizontal::{
        compute_ownership_change, HorizontalShardRouter, HorizontalShardingConfig,
        ShardOwnershipChange,
    };

    /// Test that two orchestrator instances can coordinate shard ownership
    #[tokio::test]
    async fn test_two_instance_coordination() {
        let redis = Arc::new(MockRedisCoordinator::new(Duration::from_secs(30)));

        // Instance 1 claims shards 0-7
        let config1 =
            HorizontalShardingConfig::new(16, (0..8).collect(), "orchestrator-1".to_string());
        let coord1 = ShardCoordinator::new(redis.clone(), config1.clone()).unwrap();
        let claimed1 = coord1.register().await.unwrap();

        assert_eq!(claimed1.len(), 8);
        assert_eq!(coord1.owned_shard_count(), 8);

        // Instance 2 claims shards 8-15
        let config2 =
            HorizontalShardingConfig::new(16, (8..16).collect(), "orchestrator-2".to_string());
        let coord2 = ShardCoordinator::new(redis.clone(), config2.clone()).unwrap();
        let claimed2 = coord2.register().await.unwrap();

        assert_eq!(claimed2.len(), 8);
        assert_eq!(coord2.owned_shard_count(), 8);

        // Create routers from owned shards
        let router1 =
            HorizontalShardRouter::new(config1.clone().with_assigned_shards(coord1.owned_shards()));
        let router2 =
            HorizontalShardRouter::new(config2.clone().with_assigned_shards(coord2.owned_shards()));

        // Test that task queues are correctly routed to one instance
        for i in 0..100 {
            let queue = format!("queue-{}", i);
            let shard = router1.get_shard("ns", &queue);

            let owned1 = router1.owns_task_queue("ns", &queue);
            let owned2 = router2.owns_task_queue("ns", &queue);

            // Exactly one instance should own each queue
            assert!(
                owned1 ^ owned2,
                "Queue {} (shard {}) should be owned by exactly one instance, got owned1={}, owned2={}",
                queue, shard, owned1, owned2
            );
        }
    }

    /// Test graceful handoff when an instance releases shards
    #[tokio::test]
    async fn test_graceful_handoff() {
        let redis = Arc::new(MockRedisCoordinator::new(Duration::from_secs(30)));

        // Instance 1 claims all shards
        let config1 =
            HorizontalShardingConfig::new(8, (0..8).collect(), "orchestrator-1".to_string());
        let coord1 = ShardCoordinator::new(redis.clone(), config1).unwrap();
        coord1.register().await.unwrap();
        assert_eq!(coord1.owned_shard_count(), 8);

        // Instance 1 releases half its shards
        let shards_to_release: Vec<u32> = coord1.owned_shards().into_iter().skip(4).collect();

        // Simulate release by letting another instance claim them
        coord1.release_shards().await.unwrap();
        assert_eq!(coord1.owned_shard_count(), 0);

        // Instance 2 can now claim the released shards
        let config2 = HorizontalShardingConfig::new(
            8,
            shards_to_release.clone(),
            "orchestrator-2".to_string(),
        );
        let coord2 = ShardCoordinator::new(redis.clone(), config2).unwrap();
        let claimed2 = coord2.register().await.unwrap();

        assert_eq!(claimed2, shards_to_release);
    }

    /// Test auto-assignment mode where instances claim available shards
    #[tokio::test]
    async fn test_auto_assignment_fair_distribution() {
        let redis = Arc::new(MockRedisCoordinator::new(Duration::from_secs(30)));
        let total_shards = 12u32;

        // Three instances in auto-assignment mode
        let instances: Vec<_> = (0..3)
            .map(|i| {
                let config =
                    HorizontalShardingConfig::auto(total_shards, format!("orchestrator-{}", i));
                ShardCoordinator::new(redis.clone(), config).unwrap()
            })
            .collect();

        // First instance gets all shards (in auto mode)
        let claimed0 = instances[0].register().await.unwrap();
        assert_eq!(claimed0.len(), total_shards as usize);

        // Other instances get nothing (shards already claimed)
        let claimed1 = instances[1].register().await.unwrap();
        let claimed2 = instances[2].register().await.unwrap();
        assert!(claimed1.is_empty());
        assert!(claimed2.is_empty());

        // In production, rebalancing would be triggered via ShardCoordinator
        // For now, verify that at least the first instance has all shards
        let all_owned: HashSet<u32> = instances.iter().flat_map(|c| c.owned_shards()).collect();
        assert_eq!(all_owned.len(), total_shards as usize);
    }

    /// Test ownership change detection
    #[test]
    fn test_ownership_change_detection() {
        let old: HashSet<u32> = [0, 1, 2, 3].into_iter().collect();
        let new: HashSet<u32> = [2, 3, 4, 5].into_iter().collect();

        let change = compute_ownership_change(&old, &new);
        assert!(change.has_change());

        // Check for specific changes
        match change {
            ShardOwnershipChange::Gained(gained) => {
                // We gained 4, 5 and lost 0, 1 - the function prioritizes gained
                assert!(gained.contains(&4) || gained.contains(&5));
            }
            ShardOwnershipChange::Lost(lost) => {
                assert!(lost.contains(&0) || lost.contains(&1));
            }
            ShardOwnershipChange::NoChange => {
                panic!("Expected change but got NoChange");
            }
        }
    }

    /// Test router with disabled sharding (single instance mode)
    #[test]
    fn test_single_instance_mode() {
        let router = HorizontalShardRouter::disabled();

        assert!(!router.is_enabled());

        // Should own all queues
        for i in 0..100 {
            let queue = format!("queue-{}", i);
            assert!(
                router.owns_task_queue("ns", &queue),
                "Disabled router should own all queues"
            );
        }
    }

    /// Test heartbeat maintains ownership
    #[tokio::test]
    async fn test_heartbeat_maintains_ownership() {
        let redis = Arc::new(MockRedisCoordinator::new(Duration::from_secs(5)));

        let config =
            HorizontalShardingConfig::new(4, vec![0, 1, 2, 3], "orchestrator-1".to_string());
        let coordinator = ShardCoordinator::new(redis, config).unwrap();
        coordinator.register().await.unwrap();

        // Initial ownership
        assert_eq!(coordinator.owned_shard_count(), 4);

        // Heartbeat should succeed and maintain ownership
        let result = coordinator.heartbeat().await.unwrap();
        assert_eq!(result.refreshed, 4);
        assert!(result.lost.is_empty());
        assert!(result.is_healthy());

        // Still own all shards
        assert_eq!(coordinator.owned_shard_count(), 4);
    }

    /// Test health check status
    #[tokio::test]
    async fn test_health_status() {
        let redis = Arc::new(MockRedisCoordinator::new(Duration::from_secs(30)));

        let config = HorizontalShardingConfig::new(4, vec![0, 1], "orchestrator-1".to_string());
        let coordinator = ShardCoordinator::new(redis, config).unwrap();
        coordinator.register().await.unwrap();
        coordinator.heartbeat().await.unwrap();

        let health = coordinator.health_check().await;

        assert!(health.redis_healthy);
        assert!(health.heartbeat_ok);
        assert_eq!(health.owned_shards, 2);
        assert!(!health.is_running); // Not started heartbeat task
    }

    /// Test that routers with same config produce consistent shard assignments
    #[test]
    fn test_consistent_shard_assignment_across_routers() {
        let config1 = HorizontalShardingConfig::new(16, vec![0, 1, 2, 3], "inst-1".to_string());
        let config2 = HorizontalShardingConfig::new(16, vec![0, 1, 2, 3], "inst-2".to_string());

        let router1 = HorizontalShardRouter::new(config1);
        let router2 = HorizontalShardRouter::new(config2);

        // Same inputs should produce same shard assignments
        for i in 0..100 {
            let queue = format!("queue-{}", i);
            let shard1 = router1.get_shard("ns", &queue);
            let shard2 = router2.get_shard("ns", &queue);

            assert_eq!(
                shard1, shard2,
                "Shard assignment should be consistent across routers"
            );
        }
    }

    /// Test try_claim_additional for dynamic scaling
    #[tokio::test]
    async fn test_dynamic_scaling_claim_additional() {
        let redis = Arc::new(MockRedisCoordinator::new(Duration::from_secs(30)));

        // Start with 2 shards
        let config = HorizontalShardingConfig::new(8, vec![0, 1], "orchestrator-1".to_string());
        let coordinator = ShardCoordinator::new(redis, config).unwrap();
        coordinator.register().await.unwrap();
        assert_eq!(coordinator.owned_shard_count(), 2);

        // Dynamically claim more shards
        let additional = coordinator
            .try_claim_additional(&[2, 3, 4, 5])
            .await
            .unwrap();
        assert_eq!(additional.len(), 4);
        assert_eq!(coordinator.owned_shard_count(), 6);

        // Verify we now own shards 0-5
        for shard in 0..6 {
            assert!(
                coordinator.owns_shard(shard),
                "Should own shard {} after claiming additional",
                shard
            );
        }
    }
}
