use std::str::FromStr;
use rustc_serialize::*;
use chrono::*;

#[derive(PartialEq, Eq, Debug, RustcEncodable)]
pub struct TimeStamp(DateTime<UTC>);

impl FromStr for TimeStamp {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let datetime = try!(UTC.datetime_from_str(s, "%Y-%m-%d %H:%M:%S UTC"));
        Ok(TimeStamp(datetime))
    }
}

impl ToString for TimeStamp {
    fn to_string(&self) -> String {
        let TimeStamp(datetime) = *self;
        datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string()
    }
}

impl Decodable for TimeStamp {
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        let field = try!(d.read_str());
        match field.parse() {
            Ok(result) => Ok(result),
            Err(_) => Err(d.error(&*format!("Could not parse '{}' as a TimeStamp.", field)))
        }
    }
}

#[derive(Debug, RustcDecodable, RustcEncodable)]
pub struct Table {
    pub name: String,
    pub schema: String,
    pub count: u64,
    pub created_at: TimeStamp,
    pub updated_at: TimeStamp,
    pub estimated_storage_size: u64,
    pub last_import: Option<TimeStamp>,
    pub last_log_timestamp: Option<TimeStamp>,
    pub expire_days: Option<u32>
}

#[derive(Debug, RustcDecodable, RustcEncodable)]
pub struct Tables {
    pub database: String,
    pub tables: Vec<Table>
}

#[derive(Debug, RustcDecodable, RustcEncodable)]
pub struct Database {
    pub name: String,
    pub count: u64,
    pub created_at: TimeStamp,
    pub updated_at: TimeStamp,
    pub permission: String
}

#[derive(Debug, RustcDecodable, RustcEncodable)]
pub struct Databases {
    pub databases: Vec<Database>
}

#[derive(Debug)]
pub enum JobStatus {
    Queued,
    Running,
    Success,
    Killed,
    Error
}

impl FromStr for JobStatus {
    type Err = json::DecoderError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "queued" => Ok(JobStatus::Queued),
            "running" => Ok(JobStatus::Running),
            "success" => Ok(JobStatus::Success),
            "killed" => Ok(JobStatus::Killed),
            "error" => Ok(JobStatus::Error),
            _ => Err(json::DecoderError::ExpectedError(
                    "(queued|running|success|error|killed)".to_string(), s.to_string()))
        }
    }
}

#[derive(Debug, RustcEncodable)]
pub enum JobQuery {
    Query(String),
    Config(json::Json)
}

// We can't use RustcDecodable because `query` can have either string or object...
#[derive(Debug)]
pub struct Job {
    pub job_id: u64,
    pub job_type: String,
    pub query: JobQuery,
    pub status: String,
    pub url: String,
    pub created_at: TimeStamp,
    pub start_at: Option<TimeStamp>,
    pub end_at: Option<TimeStamp>,
    pub cpu_time: Option<String>,
    pub result_size: Option<u64>,
    pub hive_result_schema: Option<Vec<Vec<String>>>,
    pub priority: u64,
    pub retry_limit: u64,
    pub duration: Option<u64>
}

#[derive(Debug)]
pub struct Jobs {
    pub count: u64,
    pub from: Option<u64>,
    pub to: Option<u64>,
    pub jobs: Vec<Job>
}

#[derive(Debug, RustcDecodable, RustcEncodable)]
pub enum QueryType {
    Hive,
    Presto,
    Pig
}

impl ToString for QueryType {
    fn to_string(&self) -> String {
        match self {
            &QueryType::Hive => "hive".to_string(),
            &QueryType::Presto => "presto".to_string(),
            &QueryType::Pig => "pig".to_string()
        }
    }
}

