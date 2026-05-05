use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "db.opendb.dev",
    version = "v1alpha1",
    kind = "OpenDbCluster",
    plural = "opendbclusters",
    namespaced,
    status = "OpenDbClusterStatus",
    derive = "PartialEq",
    shortname = "odb"
)]
#[serde(rename_all = "camelCase")]
pub struct OpenDbClusterSpec {
    pub replicas: i32,
    pub image: String,
    pub storage_class_name: String,
    pub storage_size: String,
    pub pgwire_port: i32,
    pub health_port: i32,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenDbClusterStatus {
    pub ready_replicas: i32,
    pub phase: String,
}
