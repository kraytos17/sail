use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskStreamKey, WorkerId};
use crate::proto::decode::try_decode_physical_expr;
use crate::stream::reader::TaskReadLocation;
use crate::stream::writer::{LocalStreamStorage, TaskWriteLocation};
use crate::task::gen_;

#[derive(Debug, Clone)]
pub struct TaskDefinition {
    pub plan: Arc<[u8]>,
    pub inputs: Vec<TaskInput>,
    pub output: TaskOutput,
}

#[derive(Debug, Clone)]
pub struct TaskInput {
    pub locator: TaskInputLocator,
}

#[derive(Debug, Clone)]
pub enum TaskInputLocator {
    Driver {
        stage: usize,
        keys: Vec<Vec<TaskInputKey>>,
    },
    Worker {
        stage: usize,
        keys: Vec<Vec<(WorkerId, TaskInputKey)>>,
    },
    Remote {
        uri: String,
        stage: usize,
        keys: Vec<Vec<TaskInputKey>>,
    },
}

#[derive(Debug, Clone)]
pub struct TaskInputKey {
    pub partition: usize,
    pub attempt: usize,
    pub channel: usize,
}

#[derive(Debug, Clone)]
pub struct TaskOutput {
    pub distribution: TaskOutputDistribution,
    pub locator: TaskOutputLocator,
}

#[derive(Debug, Clone)]
pub enum TaskOutputDistribution {
    Hash {
        keys: Vec<Arc<[u8]>>,
        channels: usize,
    },
    RoundRobin {
        channels: usize,
    },
    RoundRobinRow {
        channels: usize,
    },
}

#[derive(Debug, Clone)]
pub enum TaskOutputLocator {
    Local { replicas: usize },
    Remote { uri: String },
}

impl From<TaskDefinition> for gen_::TaskDefinition {
    fn from(value: TaskDefinition) -> Self {
        let TaskDefinition {
            plan,
            inputs,
            output,
        } = value;
        gen_::TaskDefinition {
            plan: plan.to_vec(),
            inputs: inputs.into_iter().map(|x| x.into()).collect(),
            output: Some(output.into()),
        }
    }
}

impl TryFrom<gen_::TaskDefinition> for TaskDefinition {
    type Error = ExecutionError;

    fn try_from(value: gen_::TaskDefinition) -> Result<Self, Self::Error> {
        let inputs = value
            .inputs
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()?;
        let output = match value.output {
            Some(x) => x.try_into()?,
            None => {
                return Err(ExecutionError::InvalidArgument(
                    "cannot decode empty task output".to_string(),
                ));
            }
        };
        Ok(TaskDefinition {
            plan: Arc::from(value.plan),
            inputs,
            output,
        })
    }
}

impl From<TaskInput> for gen_::TaskInput {
    fn from(value: TaskInput) -> Self {
        let TaskInput { locator } = value;
        gen_::TaskInput {
            locator: Some(locator.into()),
        }
    }
}

impl TryFrom<gen_::TaskInput> for TaskInput {
    type Error = ExecutionError;

    fn try_from(value: gen_::TaskInput) -> Result<Self, Self::Error> {
        let locator = match value.locator {
            Some(x) => x.try_into()?,
            None => {
                return Err(ExecutionError::InvalidArgument(
                    "cannot decode empty task input locator".to_string(),
                ));
            }
        };
        Ok(TaskInput { locator })
    }
}

impl From<TaskInputLocator> for gen_::TaskInputLocator {
    fn from(value: TaskInputLocator) -> Self {
        let kind = match value {
            TaskInputLocator::Driver { stage, keys } => {
                gen_::task_input_locator::Kind::Driver(gen_::TaskInputDriverLocator {
                    stage: stage as u64,
                    keys: keys.into_iter().map(|x| x.into()).collect(),
                })
            }
            TaskInputLocator::Worker { stage, keys } => {
                gen_::task_input_locator::Kind::Worker(gen_::TaskInputWorkerLocator {
                    stage: stage as u64,
                    keys: keys.into_iter().map(|x| x.into()).collect(),
                })
            }
            TaskInputLocator::Remote { uri, stage, keys } => {
                gen_::task_input_locator::Kind::Remote(gen_::TaskInputRemoteLocator {
                    uri,
                    stage: stage as u64,
                    keys: keys.into_iter().map(|x| x.into()).collect(),
                })
            }
        };
        gen_::TaskInputLocator { kind: Some(kind) }
    }
}

impl TryFrom<gen_::TaskInputLocator> for TaskInputLocator {
    type Error = ExecutionError;

    fn try_from(value: gen_::TaskInputLocator) -> Result<Self, Self::Error> {
        match value.kind {
            Some(gen_::task_input_locator::Kind::Driver(gen_::TaskInputDriverLocator {
                stage,
                keys,
            })) => {
                let keys = keys
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<ExecutionResult<Vec<_>>>()?;
                Ok(TaskInputLocator::Driver {
                    stage: stage as usize,
                    keys,
                })
            }
            Some(gen_::task_input_locator::Kind::Worker(gen_::TaskInputWorkerLocator {
                stage,
                keys,
            })) => {
                let keys = keys
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<ExecutionResult<Vec<_>>>()?;
                Ok(TaskInputLocator::Worker {
                    stage: stage as usize,
                    keys,
                })
            }
            Some(gen_::task_input_locator::Kind::Remote(gen_::TaskInputRemoteLocator {
                uri,
                stage,
                keys,
            })) => {
                let keys = keys
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<ExecutionResult<Vec<_>>>()?;
                Ok(TaskInputLocator::Remote {
                    uri,
                    stage: stage as usize,
                    keys,
                })
            }
            None => Err(ExecutionError::InvalidArgument(
                "cannot decode empty task input locator".to_string(),
            )),
        }
    }
}

impl From<TaskInputKey> for gen_::TaskInputDriverKey {
    fn from(value: TaskInputKey) -> Self {
        let TaskInputKey {
            partition,
            attempt,
            channel,
        } = value;
        gen_::TaskInputDriverKey {
            partition: partition as u64,
            attempt: attempt as u64,
            channel: channel as u64,
        }
    }
}

impl TryFrom<gen_::TaskInputDriverKey> for TaskInputKey {
    type Error = ExecutionError;

    fn try_from(value: gen_::TaskInputDriverKey) -> Result<Self, Self::Error> {
        Ok(TaskInputKey {
            partition: value.partition as usize,
            attempt: value.attempt as usize,
            channel: value.channel as usize,
        })
    }
}

impl From<Vec<TaskInputKey>> for gen_::TaskInputDriverKeyList {
    fn from(value: Vec<TaskInputKey>) -> Self {
        gen_::TaskInputDriverKeyList {
            keys: value.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl TryFrom<gen_::TaskInputDriverKeyList> for Vec<TaskInputKey> {
    type Error = ExecutionError;

    fn try_from(value: gen_::TaskInputDriverKeyList) -> Result<Self, Self::Error> {
        value
            .keys
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()
    }
}

impl From<(WorkerId, TaskInputKey)> for gen_::TaskInputWorkerKey {
    fn from(value: (WorkerId, TaskInputKey)) -> Self {
        let (
            worker_id,
            TaskInputKey {
                partition,
                attempt,
                channel,
            },
        ) = value;
        gen_::TaskInputWorkerKey {
            worker_id: worker_id.into(),
            partition: partition as u64,
            attempt: attempt as u64,
            channel: channel as u64,
        }
    }
}

impl TryFrom<gen_::TaskInputWorkerKey> for (WorkerId, TaskInputKey) {
    type Error = ExecutionError;

    fn try_from(value: gen_::TaskInputWorkerKey) -> Result<Self, Self::Error> {
        Ok((
            value.worker_id.into(),
            TaskInputKey {
                partition: value.partition as usize,
                attempt: value.attempt as usize,
                channel: value.channel as usize,
            },
        ))
    }
}

impl From<Vec<(WorkerId, TaskInputKey)>> for gen_::TaskInputWorkerKeyList {
    fn from(value: Vec<(WorkerId, TaskInputKey)>) -> Self {
        gen_::TaskInputWorkerKeyList {
            keys: value.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl TryFrom<gen_::TaskInputWorkerKeyList> for Vec<(WorkerId, TaskInputKey)> {
    type Error = ExecutionError;

    fn try_from(value: gen_::TaskInputWorkerKeyList) -> Result<Self, Self::Error> {
        value
            .keys
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()
    }
}

impl From<TaskInputKey> for gen_::TaskInputRemoteKey {
    fn from(value: TaskInputKey) -> Self {
        let TaskInputKey {
            partition,
            attempt,
            channel,
        } = value;
        gen_::TaskInputRemoteKey {
            partition: partition as u64,
            attempt: attempt as u64,
            channel: channel as u64,
        }
    }
}

impl TryFrom<gen_::TaskInputRemoteKey> for TaskInputKey {
    type Error = ExecutionError;

    fn try_from(value: gen_::TaskInputRemoteKey) -> Result<Self, Self::Error> {
        Ok(TaskInputKey {
            partition: value.partition as usize,
            attempt: value.attempt as usize,
            channel: value.channel as usize,
        })
    }
}

impl From<Vec<TaskInputKey>> for gen_::TaskInputRemoteKeyList {
    fn from(value: Vec<TaskInputKey>) -> Self {
        gen_::TaskInputRemoteKeyList {
            keys: value.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl TryFrom<gen_::TaskInputRemoteKeyList> for Vec<TaskInputKey> {
    type Error = ExecutionError;

    fn try_from(value: gen_::TaskInputRemoteKeyList) -> Result<Self, Self::Error> {
        value
            .keys
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()
    }
}

impl From<TaskOutput> for gen_::TaskOutput {
    fn from(value: TaskOutput) -> Self {
        let TaskOutput {
            distribution,
            locator,
        } = value;
        gen_::TaskOutput {
            distribution: Some(distribution.into()),
            locator: Some(locator.into()),
        }
    }
}

impl TryFrom<gen_::TaskOutput> for TaskOutput {
    type Error = ExecutionError;

    fn try_from(value: gen_::TaskOutput) -> Result<Self, Self::Error> {
        let distribution = match value.distribution {
            Some(x) => x.try_into()?,
            None => {
                return Err(ExecutionError::InvalidArgument(
                    "cannot decode empty task output distribution".to_string(),
                ));
            }
        };
        let locator = match value.locator {
            Some(x) => x.try_into()?,
            None => {
                return Err(ExecutionError::InvalidArgument(
                    "cannot decode empty task output locator".to_string(),
                ));
            }
        };
        Ok(TaskOutput {
            distribution,
            locator,
        })
    }
}

impl From<TaskOutputDistribution> for gen_::TaskOutputDistribution {
    fn from(value: TaskOutputDistribution) -> Self {
        let kind = match value {
            TaskOutputDistribution::Hash { keys, channels } => {
                gen_::task_output_distribution::Kind::Hash(gen_::TaskOutputHashDistribution {
                    keys: keys.into_iter().map(|k| k.to_vec()).collect(),
                    channels: channels as u64,
                })
            }
            TaskOutputDistribution::RoundRobin { channels } => {
                gen_::task_output_distribution::Kind::RoundRobin(
                    gen_::TaskOutputRoundRobinDistribution {
                        channels: channels as u64,
                    },
                )
            }
            TaskOutputDistribution::RoundRobinRow { channels } => {
                gen_::task_output_distribution::Kind::RoundRobinRow(
                    gen_::TaskOutputRoundRobinRowDistribution {
                        channels: channels as u64,
                    },
                )
            }
        };
        gen_::TaskOutputDistribution { kind: Some(kind) }
    }
}

impl TryFrom<gen_::TaskOutputDistribution> for TaskOutputDistribution {
    type Error = ExecutionError;

    fn try_from(value: gen_::TaskOutputDistribution) -> Result<Self, Self::Error> {
        match value.kind {
            Some(gen_::task_output_distribution::Kind::Hash(
                gen_::TaskOutputHashDistribution { keys, channels },
            )) => Ok(TaskOutputDistribution::Hash {
                keys: keys.into_iter().map(Arc::from).collect(),
                channels: channels as usize,
            }),
            Some(gen_::task_output_distribution::Kind::RoundRobin(
                gen_::TaskOutputRoundRobinDistribution { channels },
            )) => Ok(TaskOutputDistribution::RoundRobin {
                channels: channels as usize,
            }),
            Some(gen_::task_output_distribution::Kind::RoundRobinRow(
                gen_::TaskOutputRoundRobinRowDistribution { channels },
            )) => Ok(TaskOutputDistribution::RoundRobinRow {
                channels: channels as usize,
            }),
            None => Err(ExecutionError::InvalidArgument(
                "cannot decode empty task output distribution".to_string(),
            )),
        }
    }
}

impl From<TaskOutputLocator> for gen_::TaskOutputLocator {
    fn from(value: TaskOutputLocator) -> Self {
        let kind = match value {
            TaskOutputLocator::Local { replicas } => {
                gen_::task_output_locator::Kind::Local(gen_::TaskOutputLocalLocator {
                    replicas: replicas as u64,
                })
            }
            TaskOutputLocator::Remote { uri } => {
                gen_::task_output_locator::Kind::Remote(gen_::TaskOutputRemoteLocator { uri })
            }
        };
        gen_::TaskOutputLocator { kind: Some(kind) }
    }
}

impl TryFrom<gen_::TaskOutputLocator> for TaskOutputLocator {
    type Error = ExecutionError;

    fn try_from(value: gen_::TaskOutputLocator) -> Result<Self, Self::Error> {
        match value.kind {
            Some(gen_::task_output_locator::Kind::Local(gen_::TaskOutputLocalLocator {
                replicas,
            })) => Ok(TaskOutputLocator::Local {
                replicas: replicas as usize,
            }),
            Some(gen_::task_output_locator::Kind::Remote(gen_::TaskOutputRemoteLocator {
                uri,
            })) => Ok(TaskOutputLocator::Remote { uri }),
            None => Err(ExecutionError::InvalidArgument(
                "cannot decode empty task output locator".to_string(),
            )),
        }
    }
}

impl TaskInput {
    pub fn locations(&self, job_id: JobId) -> Vec<Vec<TaskReadLocation>> {
        match &self.locator {
            TaskInputLocator::Driver { stage, keys } => keys
                .iter()
                .map(|keys| {
                    keys.iter()
                        .map(|key| TaskReadLocation::Driver {
                            key: TaskStreamKey {
                                job_id,
                                stage: *stage,
                                partition: key.partition,
                                attempt: key.attempt,
                                channel: key.channel,
                            },
                        })
                        .collect()
                })
                .collect(),
            TaskInputLocator::Worker { stage, keys } => keys
                .iter()
                .map(|keys| {
                    keys.iter()
                        .map(|(worker_id, key)| TaskReadLocation::Worker {
                            worker_id: *worker_id,
                            key: TaskStreamKey {
                                job_id,
                                stage: *stage,
                                partition: key.partition,
                                attempt: key.attempt,
                                channel: key.channel,
                            },
                        })
                        .collect()
                })
                .collect(),
            TaskInputLocator::Remote { uri, stage, keys } => keys
                .iter()
                .map(|keys| {
                    keys.iter()
                        .map(|key| TaskReadLocation::Remote {
                            uri: uri.clone(),
                            key: TaskStreamKey {
                                job_id,
                                stage: *stage,
                                partition: key.partition,
                                attempt: key.attempt,
                                channel: key.channel,
                            },
                        })
                        .collect()
                })
                .collect(),
        }
    }
}

impl TaskOutput {
    pub fn channels(&self) -> usize {
        match self.distribution {
            TaskOutputDistribution::Hash { channels, .. } => channels,
            TaskOutputDistribution::RoundRobin { channels, .. } => channels,
            TaskOutputDistribution::RoundRobinRow { channels, .. } => channels,
        }
    }

    pub fn locations(&self, key: &TaskKey) -> Vec<TaskWriteLocation> {
        let channels = self.channels();
        match &self.locator {
            TaskOutputLocator::Local { replicas } => (0..channels)
                .map(|channel| TaskWriteLocation::Local {
                    storage: LocalStreamStorage::Memory {
                        replicas: *replicas,
                    },
                    key: TaskStreamKey {
                        job_id: key.job_id,
                        stage: key.stage,
                        partition: key.partition,
                        attempt: key.attempt,
                        channel,
                    },
                })
                .collect(),
            TaskOutputLocator::Remote { uri } => (0..channels)
                .map(|channel| TaskWriteLocation::Remote {
                    uri: uri.clone(),
                    key: TaskStreamKey {
                        job_id: key.job_id,
                        stage: key.stage,
                        partition: key.partition,
                        attempt: key.attempt,
                        channel,
                    },
                })
                .collect(),
        }
    }

    pub fn partitioning(
        &self,
        ctx: &TaskContext,
        schema: &Schema,
        codec: &dyn PhysicalExtensionCodec,
    ) -> ExecutionResult<Partitioning> {
        match &self.distribution {
            TaskOutputDistribution::Hash { keys, channels } => {
                let keys = keys
                    .iter()
                    .map(|k| {
                        try_decode_physical_expr(ctx, codec, k.as_ref(), schema)
                            .map_err(|e| e.into())
                    })
                    .collect::<ExecutionResult<Vec<_>>>()?;
                Ok(Partitioning::Hash(keys, *channels))
            }
            TaskOutputDistribution::RoundRobin { channels }
            | TaskOutputDistribution::RoundRobinRow { channels } => {
                Ok(Partitioning::RoundRobinBatch(*channels))
            }
        }
    }

    pub fn row_based(&self) -> bool {
        matches!(
            &self.distribution,
            TaskOutputDistribution::RoundRobinRow { .. }
        )
    }
}
