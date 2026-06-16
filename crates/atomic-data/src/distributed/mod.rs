use rkyv::{de::pooling::Pool, rancor::Error, ser::allocator::ArenaHandle, util::AlignedVec};

mod capabilities;
mod envelope;
mod transport;
mod wire;

pub use capabilities::*;
pub use envelope::*;
pub use transport::*;
pub use wire::*;

pub type RkyvWireSerializer<'a> =
    rkyv::api::high::HighSerializer<AlignedVec, ArenaHandle<'a>, Error>;
pub type RkyvWireValidator<'a> = rkyv::api::high::HighValidator<'a, Error>;
pub type RkyvWireStrategy = rkyv::rancor::Strategy<Pool, Error>;

/// Semantic version for wire contracts used by distributed task transport.
pub const WIRE_SCHEMA_V1: u16 = 1;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_envelope_round_trips_with_rkyv() {
        let ops = vec![PipelineOp {
            op_id: "mycrate::double".to_string(),
            action: TaskAction::Map,
            runtime: TaskRuntime::Native,
            payload: vec![],
        }];
        let envelope = TaskEnvelope::new(1, 2, 3, 0, 4, "trace-1".to_string(), ops, vec![1, 2, 3]);
        let bytes = envelope.encode_wire().expect("serialize envelope");
        let decoded = TaskEnvelope::decode_wire(&bytes).expect("deserialize envelope");
        assert_eq!(decoded.ops.len(), 1);
        assert_eq!(decoded.ops[0].op_id, "mycrate::double");
        assert_eq!(decoded.ops[0].action, TaskAction::Map);
        assert_eq!(decoded.data, vec![1, 2, 3]);
    }

    #[test]
    fn task_result_round_trips() {
        let result =
            TaskResultEnvelope::ok(1, 2, 3, 0, 0, "worker-1".to_string(), vec![4, 5, 6], None);
        let bytes = result.encode_wire().expect("serialize result");
        let decoded = TaskResultEnvelope::decode_wire(&bytes).expect("deserialize result");
        assert_eq!(decoded.status, ResultStatus::Success);
        assert_eq!(decoded.data, vec![4, 5, 6]);
        assert_eq!(decoded.worker_id, "worker-1");
    }

    #[test]
    fn fold_action_round_trips() {
        let ops = vec![PipelineOp {
            op_id: "mycrate::sum".to_string(),
            action: TaskAction::Fold,
            runtime: TaskRuntime::Native,
            payload: 0_i32.to_le_bytes().to_vec(),
        }];
        let envelope = TaskEnvelope::new(1, 2, 3, 0, 4, "trace-2".to_string(), ops, vec![1, 2, 3]);
        let bytes = envelope.encode_wire().expect("serialize");
        let decoded = TaskEnvelope::decode_wire(&bytes).expect("deserialize");
        assert_eq!(decoded.ops[0].action, TaskAction::Fold);
        assert_eq!(decoded.ops[0].payload, 0_i32.to_le_bytes().to_vec());
    }

    #[test]
    fn shuffle_map_carries_ids() {
        let ops = vec![PipelineOp {
            op_id: "sys.shuffle_map".to_string(),
            action: TaskAction::ShuffleMap {
                shuffle_id: 7,
                num_output_partitions: 4,
            },
            runtime: TaskRuntime::Native,
            payload: vec![],
        }];
        let envelope = TaskEnvelope::new(1, 2, 3, 0, 4, "trace-3".to_string(), ops, vec![]);
        let bytes = envelope.encode_wire().expect("serialize");
        let decoded = TaskEnvelope::decode_wire(&bytes).expect("deserialize");
        assert!(matches!(
            decoded.ops[0].action,
            TaskAction::ShuffleMap {
                shuffle_id: 7,
                num_output_partitions: 4
            }
        ));
    }

    #[test]
    fn multi_op_pipeline_round_trips() {
        let ops = vec![
            PipelineOp {
                op_id: "myapp::double".to_string(),
                action: TaskAction::Map,
                runtime: TaskRuntime::Native,
                payload: vec![],
            },
            PipelineOp {
                op_id: "myapp::is_positive".to_string(),
                action: TaskAction::Filter,
                runtime: TaskRuntime::Native,
                payload: vec![],
            },
            PipelineOp {
                op_id: "myapp::add".to_string(),
                action: TaskAction::Fold,
                runtime: TaskRuntime::Native,
                payload: 0_i32.to_le_bytes().to_vec(),
            },
        ];
        let envelope = TaskEnvelope::new(1, 2, 3, 0, 0, "trace-multi".to_string(), ops, vec![]);
        let bytes = envelope.encode_wire().expect("serialize");
        let decoded = TaskEnvelope::decode_wire(&bytes).expect("deserialize");
        assert_eq!(decoded.ops.len(), 3);
        assert_eq!(decoded.ops[0].op_id, "myapp::double");
        assert_eq!(decoded.ops[1].action, TaskAction::Filter);
        assert_eq!(decoded.ops[2].op_id, "myapp::add");
    }

    #[test]
    fn default_runtime_is_native() {
        let op = PipelineOp {
            op_id: "x".to_string(),
            action: TaskAction::Map,
            runtime: TaskRuntime::default(),
            payload: vec![],
        };
        assert_eq!(op.runtime, TaskRuntime::Native);
    }

    #[test]
    fn native_runtime_roundtrips_rkyv() {
        let ops = vec![PipelineOp {
            op_id: "x".to_string(),
            action: TaskAction::Map,
            runtime: TaskRuntime::Native,
            payload: vec![],
        }];
        let envelope = TaskEnvelope::new(1, 2, 3, 0, 0, "t".to_string(), ops, vec![]);
        let bytes = envelope.encode_wire().unwrap();
        let decoded = TaskEnvelope::decode_wire(&bytes).unwrap();
        assert_eq!(decoded.ops[0].runtime, TaskRuntime::Native);
    }

    #[test]
    fn task_action_variants_exhaustive() {
        let action = TaskAction::Map;
        let _ = match action {
            TaskAction::Map
            | TaskAction::Filter
            | TaskAction::FlatMap
            | TaskAction::Fold
            | TaskAction::Reduce
            | TaskAction::Aggregate
            | TaskAction::Collect
            | TaskAction::ShuffleMap { .. }
            | TaskAction::Cache { .. }
            | TaskAction::ReadFileSplit => true,
            #[cfg(feature = "kafka")]
            TaskAction::KafkaConsume => true,
        };
    }

    #[test]
    fn worker_caps_round_trips() {
        let ops = vec!["myapp::double".to_string(), "myapp::sum".to_string()];
        let caps = WorkerCapabilities::new("worker-42".to_string(), 8, ops.clone());
        let bytes = caps.encode_wire().expect("serialize caps");
        let decoded = WorkerCapabilities::decode_wire(&bytes).expect("deserialize caps");
        assert_eq!(decoded.worker_id, "worker-42");
        assert_eq!(decoded.max_tasks, 8);
        assert_eq!(decoded.registered_ops, ops);
    }

    #[test]
    fn legacy_frame_ids_rejected() {
        assert!(TransportFrameKind::try_from(1).is_err());
        assert!(TransportFrameKind::try_from(2).is_err());
    }

    #[test]
    fn rkyv_codec_round_trips() {
        let encoded = vec![1_u32, 2, 3].encode_wire().expect("serialize values");
        let decoded = Vec::<u32>::decode_wire(&encoded).expect("deserialize values");
        assert_eq!(decoded, vec![1, 2, 3]);
    }
}
