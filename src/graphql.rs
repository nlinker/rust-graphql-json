use crate::graphql_json::GraphQLJson;
use juniper::graphql_object;
use juniper::{EmptyMutation, EmptySubscription, FieldError, GraphQLEnum, RootNode};
use sqlx::PgPool;
use sqlx::types::chrono::{DateTime, Utc};
use std::fmt;
use std::collections::HashMap;
use crate::graphql_map::GraphQLMap;
use futures::TryFutureExt;


#[derive(Clone, Debug)]
pub struct Context {
    pub pg_pool: PgPool,
}

impl juniper::Context for Context {}

#[derive(Clone, Copy, Debug, GraphQLEnum)]
pub enum UserKind {
    Admin,
    User,
    Guest,
}

#[derive(Clone, Debug)]
pub struct User {
    pub id: i32,
    pub kind: UserKind,
    pub name: String,
    pub json: GraphQLJson,
}

#[graphql_object(Context = Context)]
impl User {
    fn id(&self) -> i32 {
        self.id
    }

    fn kind(&self) -> UserKind {
        self.kind
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn json(&self) -> &GraphQLJson {
        &self.json
    }

    async fn friends(&self) -> Vec<User> {
        vec![]
    }
}

#[derive(Clone, Copy, Debug)]
pub struct QueryRoot;

#[graphql_object(Context = Context)]
impl QueryRoot {

    /// Fetch the list of jobs
    async fn jobs_by_ids(
        context: &Context,
        /// The list of command ids to fetch, ids may be empty
        ids: Vec<i32>,
    ) -> juniper::FieldResult<Vec<Job0>> {
        let ids: &[i32] = &ids[..];

        let unordered_jobs: Vec<Job0> = sqlx::query_as!(
            JobRecord,
            r#"
                SELECT job.id,
                       array(SELECT cj.command_id
                             FROM chroma_core_command_jobs AS cj
                             WHERE cj.job_id = job.id) :: INT[] AS commands,
                       job.state,
                       job.errored,
                       job.cancelled,
                       job.modified_at,
                       job.created_at,
                       job.wait_for_json,
                       job.locks_json,
                       job.content_type_id,
                       job.class_name,
                       job.raw_description,
                       job.raw_cancellable,
                       array_agg(sr.id)::INT[]                  AS step_result_keys,
                       array_agg(sr.result)::TEXT[]             AS step_results_values
                FROM chroma_core_job AS job
                         JOIN chroma_core_command_jobs AS cj ON cj.job_id = job.id
                         JOIN chroma_core_stepresult AS sr ON sr.job_id = job.id
                WHERE (job.id = ANY ($1::INT[]))
                GROUP BY job.id
            "#,
            ids
        )
        .fetch_all(&context.pg_pool)
        .map_ok(|xs: Vec<JobRecord>|
            xs.into_iter().filter_map(|x| convert_to_job0(x).ok()).collect::<Vec<Job0>>()
        )
        .await?;

        let mut hm = unordered_jobs
            .into_iter()
            .map(|x| (x.id, x))
            .collect::<HashMap<i32, Job0>>();
        let mut not_found = Vec::new();
        let jobs = ids
            .iter()
            .filter_map(|id| {
                hm.remove(id).or_else(|| {
                    not_found.push(id);
                    None
                })
            })
            .collect::<Vec<Job0>>();

        if !not_found.is_empty() {
            Err(FieldError::from(format!("Jobs not found for ids: {:?}", not_found)))
        } else {
            Ok(jobs)
        }
    }

    /// Fetch a URL and return the response body text.
    async fn request(url: String) -> Result<String, FieldError> {
        Ok(reqwest::get(&url).await?.text().await?)
    }
}

pub type Schema = RootNode<'static, QueryRoot, EmptyMutation<Context>, EmptySubscription<Context>>;

pub fn schema() -> Schema {
    Schema::new(
        QueryRoot,
        EmptyMutation::<Context>::new(),
        EmptySubscription::<Context>::new(),
    )
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[derive(juniper::GraphQLObject)]
// #[cfg_attr(feature = "graphql", derive(juniper::GraphQLObject))]
pub struct AvailableTransition {
    pub label: String,
    pub state: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[derive(juniper::GraphQLObject)]
// #[cfg_attr(feature = "graphql", derive(juniper::GraphQLObject))]
pub struct JobLock {
    pub locked_item_content_type_id: i32,
    pub locked_item_id: i32,
    pub locked_item_uri: String,
    pub resource_uri: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[derive(juniper::GraphQLObject)]
pub struct JobLockUnresolved {
    pub uuid: String,
    pub locked_item_id: i32,
    pub locked_item_type_id: i32,
    pub write: bool,
    pub begin_state: Option<String>,
    pub end_state: Option<String>,
}

/// Concrete version of `iml_wire_types::Job<T>` needed for GraphQL
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[derive(juniper::GraphQLObject)]
// #[cfg_attr(feature = "graphql", derive(juniper::GraphQLObject))]
pub struct Job0 {
    pub available_transitions: Vec<AvailableTransition>,
    pub cancelled: bool,
    pub class_name: String,
    pub created_at: String,
    pub description: String,
    pub errored: bool,
    pub id: i32,
    pub modified_at: String,
    pub resource_uri: String,
    pub state: String,
    pub step_results: GraphQLMap,
    pub steps: Vec<String>,
    pub wait_for: Vec<String>,
    pub commands: Vec<String>,
    pub read_locks: Vec<JobLock>,
    pub write_locks: Vec<JobLock>,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
struct JobRecord {
    id: i32,
    commands: Option<Vec<i32>>,
    state: String,
    errored: bool,
    cancelled: bool,
    modified_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    wait_for_json: String,
    locks_json: String,
    content_type_id: Option<i32>,
    class_name: String,
    raw_description: String,
    raw_cancellable: bool,
    step_result_keys: Option<Vec<i32>>,
    step_results_values: Option<Vec<String>>,
}

fn convert_to_job0(x: JobRecord) -> juniper::FieldResult<Job0> {
    let available_transitions =
        if x.state == "complete" || !x.raw_cancellable {
            vec![]
        } else {
            vec![AvailableTransition {
                label: "Cancel".to_string(),
                state: "cancelled".to_string(),
            }]
        };
    let (read_locks, write_locks) = convert_job_locks(&x.locks_json)?;
    let commands = x.commands
        .unwrap_or_default()
        .into_iter()
        .map(|id| format!("/api/{}/{}/", "command", id)) // TODO
        .collect();
    let wait_for = convert_job_wait_for(&x.wait_for_json)?;
    let step_results = convert_job_step_results(&x.step_result_keys, &x.step_results_values)?;
    let steps = x.step_result_keys
        .unwrap_or_default()
        .iter()
        .map(|id| format!("/api/{}/{}/", "step", id)) // TODO
        .collect();
    let job0 = Job0 {
        available_transitions,
        id: x.id,
        state: x.state,
        class_name: x.class_name,
        cancelled: x.cancelled,
        errored: x.errored,
        modified_at: x.modified_at.format("%Y-%m-%dT%T%.6f").to_string(),
        created_at: x.created_at.format("%Y-%m-%dT%T%.6f").to_string(),
        resource_uri: format!("/api/{}/{}/", "job", x.id), // TODO
        description: x.raw_description,
        step_results,
        steps,
        wait_for,
        commands,
        read_locks,
        write_locks,
    };
    Ok(job0)
}

fn convert_job_step_results(
    keys: &Option<Vec<i32>>,
    values: &Option<Vec<String>>,
) -> juniper::FieldResult<GraphQLMap> {
    let mut hm = HashMap::new();
    if let Some(keys) = keys {
        if let Some(values) = values {
            // we expect keys and values of the same size
            let n = usize::min(keys.len(), values.len());
            for i in 0..n {
                let k = format!("/api/{}/{}/", "step", keys[i]);
                let v = serde_json::from_str::<serde_json::Value>(&values[i])?;
                hm.insert(k, v);
            }
        }
    }
    Ok(GraphQLMap(hm))
}

fn convert_job_locks(locks_json: &str) -> juniper::FieldResult<(Vec<JobLock>, Vec<JobLock>)> {
    let locks = serde_json::from_str::<serde_json::Value>(locks_json)?;
    let mut read_locks = vec![];
    let mut write_locks = vec![];
    if let serde_json::Value::Array(locks_raw) = locks {
        for lock_raw in locks_raw {
            let lock_unresolved = serde_json::from_value::<JobLockUnresolved>(lock_raw)?;
            let item_type = convert_to_item_type(lock_unresolved.locked_item_type_id)?;
            let locked_item_id = lock_unresolved.locked_item_id;
            let locked_item_uri = item_type_to_uri(item_type, locked_item_id);
            let resource_uri = "".to_string();
            let lock = JobLock {
                locked_item_content_type_id: lock_unresolved.locked_item_type_id,
                locked_item_id,
                locked_item_uri,
                resource_uri,
            };
            if lock_unresolved.write {
                write_locks.push(lock);
            } else {
                read_locks.push(lock);
            }
        }
    };
    Ok((read_locks, write_locks))
}

fn convert_job_wait_for(json: &str) -> juniper::FieldResult<Vec<String>> {
    // raw_ids is like "[6, 7, 8, 9, 12, 13, 14]"
    let ids = serde_json::from_str::<serde_json::Value>(json)?;
    if let serde_json::Value::Array(ids) = ids {
        let wait_for = ids.into_iter()
            .flat_map(|x| x.as_i64())
            .map(|x| format!("/api/{}/{}/", "job", x))// TODO
            .collect::<Vec<String>>();
        Ok(wait_for)
    } else {
        Err(juniper::FieldError::from(format!("Expected json array in '{}'", json)))
    }
}

fn convert_to_item_type(id: i32) -> juniper::FieldResult<LockedItemType> {
    match id {
        26 => Ok(LockedItemType::Copytool),
        77 => Ok(LockedItemType::Corosync2Configuration),
        60 => Ok(LockedItemType::CorosyncConfiguration),
        118 => Ok(LockedItemType::FilesystemTicket),
        55 => Ok(LockedItemType::LNetConfiguration),
        41 => Ok(LockedItemType::LustreClientMount),
        27 => Ok(LockedItemType::ManagedFilesystem),
        19 => Ok(LockedItemType::ManagedHost),
        38 => Ok(LockedItemType::ManagedMdt),
        32 => Ok(LockedItemType::ManagedMgs),
        54 => Ok(LockedItemType::ManagedOst),
        15 => Ok(LockedItemType::ManagedTarget),
        122 => Ok(LockedItemType::MasterTicket),
        25 => Ok(LockedItemType::NTPConfiguration),
        65 => Ok(LockedItemType::PacemakerConfiguration),
        52 => Ok(LockedItemType::StratagemConfiguration),
        42 => Ok(LockedItemType::Ticket),
        _ => Err(juniper::FieldError::from(format!("Unknown lock type with id={}", id))),
    }
}

fn item_type_to_uri(lit: LockedItemType, id: i32) -> String {
    let resource_uri = match lit {
        LockedItemType::Copytool => "copytool",
        LockedItemType::Corosync2Configuration => "corosync_configuration",
        LockedItemType::CorosyncConfiguration => "corosync_configuration",
        LockedItemType::FilesystemTicket => "ticket",
        LockedItemType::LNetConfiguration => "lnet_configuration",
        LockedItemType::LustreClientMount => "client_mount",
        LockedItemType::ManagedFilesystem => "filesystem",
        LockedItemType::ManagedHost => "host",
        LockedItemType::ManagedMdt => "target",
        LockedItemType::ManagedMgs => "target",
        LockedItemType::ManagedOst => "target",
        LockedItemType::ManagedTarget => "target",
        LockedItemType::MasterTicket => "ticket",
        LockedItemType::NTPConfiguration => "ntp_configuration",
        LockedItemType::PacemakerConfiguration => "pacemaker_configuration",
        LockedItemType::StratagemConfiguration => "stratagem_configuration",
        LockedItemType::Ticket => "ticket",
    };
    format!("/api/{}/{}/", resource_uri, id)
}

// all derived classes of StatefulObject in Chroma
#[derive(Clone, Copy, Debug)]
pub enum LockedItemType {
    Copytool,
    Corosync2Configuration,
    CorosyncConfiguration,
    FilesystemTicket,
    LNetConfiguration,
    LustreClientMount,
    ManagedFilesystem,
    ManagedHost,
    ManagedMdt,
    ManagedMgs,
    ManagedOst,
    ManagedTarget,
    MasterTicket,
    NTPConfiguration,
    PacemakerConfiguration,
    StratagemConfiguration,
    Ticket,
}

impl fmt::Display for LockedItemType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
