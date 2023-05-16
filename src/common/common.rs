#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Stage {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(oneof = "stage::StageType", tags = "2, 3, 4")]
    pub stage_type: ::core::option::Option<stage::StageType>,
    #[prost(oneof = "stage::ActionField", tags = "5")]
    pub action_field: ::core::option::Option<stage::ActionField>,
}
/// Nested message and enum types in `Stage`.
pub mod stage {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum StageType {
        #[prost(message, tag = "2")]
        Filter(super::Filter),
        #[prost(message, tag = "3")]
        Select(super::Select),
        #[prost(enumeration = "super::ActionType", tag = "4")]
        Action(i32),
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ActionField {
        #[prost(message, tag = "5")]
        SumCol(super::SumCol),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct S3Configuration {
    #[prost(string, tag = "1")]
    pub region: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub endpoint: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub access_key: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub secret_key: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Filter {
    #[prost(string, tag = "1")]
    pub predicate: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Select {
    #[prost(string, repeated, tag = "1")]
    pub columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SumCol {
    #[prost(string, tag = "1")]
    pub field_name: ::prost::alloc::string::String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ActionType {
    Sum = 0,
    Count = 1,
    Collect = 2,
    Width = 3,
}
impl ActionType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ActionType::Sum => "Sum",
            ActionType::Count => "Count",
            ActionType::Collect => "Collect",
            ActionType::Width => "Width",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Sum" => Some(Self::Sum),
            "Count" => Some(Self::Count),
            "Collect" => Some(Self::Collect),
            "Width" => Some(Self::Width),
            _ => None,
        }
    }
}
