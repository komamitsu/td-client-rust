use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufWriter;
use std::str::FromStr;
use std::time::Duration;
use std::thread;
use flate2::read::GzDecoder;
use hyper::client::{RequestBuilder, Body};
use hyper::client::response::Response;
use hyper::header::{Authorization, ContentType, ContentLength};
use hyper::mime::{Mime, TopLevel, SubLevel};
use msgpack::decode::*;
use msgpack::value::*;
use regex::Regex;
use rustc_serialize::*;
use rustc_serialize::json::{DecoderError, Json, ToJson};

use model::*;
use error::*;

const DEFAULT_API_ENDPOINT: &'static str = "https://api.treasuredata.com";
const DEFAULT_API_IMPORT_ENDPOINT: &'static str = "https://api-import.treasuredata.com";

pub struct Client <R: RequestExecutor> {
    request_exec: R,
    pub apikey: String,
    pub endpoint: String,
    pub import_endpoint: String,
    http_client: ::hyper::client::Client
}

struct ContentBody<'a>(u64, &'a mut Read);

impl <'a> From<ContentBody<'a>> for Body<'a> {
    fn from(content_body: ContentBody<'a>) -> Body<'a> {
        let ContentBody(size, read) = content_body;
        Body::SizedBody(read, size)
    }
}

pub enum JobStatusOption {
    Queued,
    Running,
    Success,
    Error
}

pub trait RequestExecutor {
    fn get_response(&self, request_builder: RequestBuilder)
        -> Result<Response, TreasureDataError>;
}

pub struct DefaultRequestExecutor {
    apikey: String
}

impl DefaultRequestExecutor {
    pub fn new(apikey: &str) -> Self {
        DefaultRequestExecutor {
            apikey: apikey.to_string()
        }
    }
}

impl RequestExecutor for DefaultRequestExecutor {
    fn get_response(&self, request_builder: RequestBuilder)
        -> Result<Response, TreasureDataError> {

        let mut res =
            try!(
                request_builder.
                header(Authorization(format!("TD1 {}", self.apikey).as_str().to_owned())).
                send()
            );
        match res.status {
            ::hyper::status::StatusCode::Ok => {
                Ok(res)
            },
            _ => {
                let mut s = String::new();
                try!(res.read_to_string(&mut s));
                Err(TreasureDataError::ApiError(res.status, s))
            }
        }
    }
}

impl Client <DefaultRequestExecutor> {
    pub fn new(apikey: &str) -> Client<DefaultRequestExecutor> {
        Client {
            request_exec: DefaultRequestExecutor::new(apikey),
            apikey: apikey.to_string(),
            endpoint: DEFAULT_API_ENDPOINT.to_string(),
            import_endpoint: DEFAULT_API_IMPORT_ENDPOINT.to_string(),
            http_client: ::hyper::Client::new()
        }
    }
}

impl <R> Client <R> where R: RequestExecutor {
    pub fn new_with_request_executor<RR>(apikey: &str, request_exec: RR) -> Client<RR>
        where RR: RequestExecutor {

        Client {
            request_exec: request_exec,
            apikey: apikey.to_string(),
            endpoint: DEFAULT_API_ENDPOINT.to_string(),
            import_endpoint: DEFAULT_API_IMPORT_ENDPOINT.to_string(),
            http_client: ::hyper::Client::new()
        }
    }

    pub fn endpoint(&mut self, endpoint: &str) -> &Self {
        self.endpoint = self.endpoint_with_protocol(endpoint);
        self
    }

    pub fn import_endpoint(&mut self, endpoint: &str) -> &Self {
        self.import_endpoint = self.endpoint_with_protocol(endpoint);
        self
    }

    fn endpoint_with_protocol(&self, endpoint: &str) -> String {
        if Regex::new("^(http://|https://)").unwrap().is_match(endpoint) {
            endpoint.to_string()
        }
        else {
            format!("https://{}", endpoint).to_string()
        }
    }

    fn get_response(&self, request_builder: RequestBuilder)
                    -> Result<Response, TreasureDataError> {
        self.request_exec.get_response(request_builder)
    }

    fn get_response_as_string(&self, request_builder: RequestBuilder)
                    -> Result<String, TreasureDataError> {
        let result: Result<Response, TreasureDataError> = self.get_response(request_builder);
        match result {
            Ok(mut res) => {
                let mut s = String::new();
                try!(res.read_to_string(&mut s));
                Ok(s)
            },
            Err(err) => {
                Err(err)
            }
        }
    }

    // Database API
    pub fn databases(&self) -> Result<Vec<Database>, TreasureDataError> {
        let response_body = try!(
            self.get_response_as_string(
                self.http_client.
                    get(format!("{}/v3/database/list",
                                self.endpoint).as_str())
            )
        );
        let databases: Databases = try!(json::decode(&response_body));
        Ok(databases.databases)
    }

    pub fn create_database(&self, name: &str) -> Result<(), TreasureDataError> {
        try!(
            self.get_response_as_string(
                self.http_client.
                    post(format!("{}/v3/database/create/{}",
                                 self.endpoint, name).as_str())
            )
        );
        Ok(())
    }

    pub fn delete_database(&self, name: &str) -> Result<(), TreasureDataError> {
        try!(
            self.get_response_as_string(
                self.http_client.
                    post(format!("{}/v3/database/delete/{}",
                                 self.endpoint, name).as_str())
            )
        );
        Ok(())
    }

    // Table API
    pub fn tables(&self, database_name: &str)-> Result<Vec<Table>, TreasureDataError> {
        let response_body = try!(
            self.get_response_as_string(
                self.http_client.
                    get(format!("{}/v3/table/list/{}",
                                self.endpoint, database_name).as_str())
            )
        );
        let tables: Tables = try!(json::decode(&response_body));
        Ok(tables.tables)
    }

    pub fn tail_table(&self, database_name: &str, name: &str) ->
                      Result<(), TreasureDataError> {
        try!(
            self.get_response_as_string(
                self.http_client.
                    get(format!("{}/v3/table/tail/{}/{}",
                                 self.endpoint, database_name, name).as_str())
            )
        );
        Ok(())
    }

    pub fn create_table(&self, database_name: &str, name: &str)
                        -> Result<(), TreasureDataError> {
        try!(
            self.get_response_as_string(
                self.http_client.
                    post(format!("{}/v3/table/create/{}/{}/log",
                                 self.endpoint, database_name, name).as_str())
            )
        );
        Ok(())
    }

    pub fn delete_table(&self, database_name: &str, name: &str)
                        -> Result<(), TreasureDataError> {
        try!(
            self.get_response_as_string(
                self.http_client.
                    post(format!("{}/v3/table/delete/{}/{}",
                                 self.endpoint, database_name, name).as_str())
            )
        );
        Ok(())
    }

    pub fn rename_table(&self, database_name: &str, name: &str, new_name: &str)
                        -> Result<(), TreasureDataError> {
        try!(
            self.get_response_as_string(
                self.http_client.
                    post(format!("{}/v3/table/rename/{}/{}/{}",
                                 self.endpoint, database_name, name, new_name).as_str())
            )
        );
        Ok(())
    }

    pub fn swap_table(&self, database_name: &str, name_a: &str, name_b: &str)
                      -> Result<(), TreasureDataError> {
        try!(
            self.get_response_as_string(
                self.http_client.
                    post(format!("{}/v3/table/swap/{}/{}/{}",
                                 self.endpoint, database_name, name_a, name_b).as_str())
            )
        );
        Ok(())
    }

    pub fn import_msgpack_gz_to_table(&self, database_name: &str, name: &str,
                        data_len: u64, data: &mut Read, unique_id: Option<&str>)
                        -> Result<(), TreasureDataError> {
        let url = match unique_id {
            Some(unique_id) => format!("{}/v3/table/import_with_id/{}/{}/{}/msgpack.gz",
                                       self.endpoint, database_name, name, unique_id),
            None => format!("{}/v3/table/import/{}/{}/msgpack.gz",
                            self.endpoint, database_name, name)
        };
        try!(
            self.get_response_as_string(
                self.http_client.put(url.as_str()).body(ContentBody(data_len, data))
            )
        );
        Ok(())
    }

    pub fn import_msgpack_gz_file_to_table(&self, database_name: &str, name: &str,
                        file_path: &str, unique_id: Option<&str>)
                        -> Result<(), TreasureDataError> {
        self.import_msgpack_gz_to_table(database_name, name,
                                         try!(fs::metadata(file_path)).len(),
                                         &mut try!(File::open(file_path)), unique_id)
    }

    fn decode_job(&self, job_json: &json::Json) -> Result<Job, TreasureDataError> {
        let hive_result_schema_opt_array: Option<Vec<Vec<String>>> =
            match pick_opt_string_item!(job_json, "hive_result_schema") {
                None => None,
                Some(s) => Some(try!(json::decode::<Vec<Vec<String>>>(s.as_str())))
            };

        let query: JobQuery =
            try!(
                job_json.
                find("query").
                ok_or(DecoderError::MissingFieldError("query".to_string())).
                and_then(|json|
                     if json.is_string() {
                         json.as_string().ok_or(expected_err!(json, "query", "String")).
                         and_then(|s| Ok(JobQuery::Query(s.to_string())))
                     }
                     else {
                         Ok(JobQuery::Config(json.clone()))
                     }
                )
            );

        Ok(Job {
            job_id: pick_string_item!(job_json, "job_id").parse().unwrap(),
            job_type: pick_string_item!(job_json, "type"),
            query: query,
            status: pick_string_item!(job_json, "status"),
            url: pick_string_item!(job_json, "url"),
            cpu_time: pick_opt_string_item!(job_json, "cpu_time"),
            result_size: pick_opt_u64_item!(job_json, "result_size"),
            created_at: pick_timestamp_item!(job_json, "created_at"),
            start_at: pick_opt_timestamp_item!(job_json, "start_at"),
            end_at: pick_opt_timestamp_item!(job_json, "end_at"),
            hive_result_schema: hive_result_schema_opt_array,
            priority: pick_u64_item!(job_json, "priority"),
            retry_limit: pick_u64_item!(job_json, "retry_limit"),
            duration: pick_opt_u64_item!(job_json, "duration")
        })
    }

    // Job API
    pub fn jobs(&self, status: Option<JobStatusOption>, from: Option<u64>, to: Option<u64>
               )-> Result<Jobs, TreasureDataError> {
        let mut params: Vec<String> = vec![];
        match status {
            Some(status) =>
                params.push(
                    format!("status={}",
                        match status {
                            JobStatusOption::Queued => "queued",
                            JobStatusOption::Running => "running",
                            JobStatusOption::Success => "success",
                            JobStatusOption::Error => "error"
                })),
            None => ()
        }
        match from {
            Some(x) => params.push(format!("from={}", x)),
            None => ()
        }
        match to {
            Some(x) => params.push(format!("to={}", x)),
            None => ()
        }
        let joined_params = params.join("&");
        let body = joined_params.as_str();
        let response_body = try!(
            self.get_response_as_string(
                self.http_client.
                    get(format!("{}/v3/job/list{}{}",
                                self.endpoint,
                                if body.len() == 0 { "" } else { "?" },
                                body).as_str())
            )
        );
        
        let response_json = try!(json::Json::from_str(&response_body));
        let jobs_json = pick_item!(response_json, "jobs", as_array, "Array");
        let count = pick_u64_item!(response_json, "count");
        let from = pick_opt_u64_item!(response_json, "from");
        let to = pick_opt_u64_item!(response_json, "to");

        let mut jobs = Vec::<Job>::new();
        for job_json in jobs_json {
            jobs.push(try!(self.decode_job(job_json)))
        }

        let result: Jobs = Jobs { count: count, from: from, to: to, jobs: jobs};
        Ok(result)
    }

    pub fn job(&self, job_id: u64) -> Result<Job, TreasureDataError> {
        let response_body = try!(
            self.get_response_as_string(
                self.http_client.
                    get(format!("{}/v3/job/show/{}", self.endpoint, job_id).as_str())
            )
        );
        let job_json: json::Json = try!(json::Json::from_str(response_body.as_str()));
        let job: Job = try!(self.decode_job(&job_json));
        Ok(job)
    }

    pub fn job_status(&self, job_id: u64) -> Result<JobStatus, TreasureDataError> {
        let response_body = try!(
            self.get_response_as_string(
                self.http_client.
                    get(format!("{}/v3/job/status/{}", self.endpoint, job_id).as_str())
            )
        );
        let job_json: json::Json = try!(json::Json::from_str(response_body.as_str()));
        let status: String = pick_string_item!(job_json, "status");
        Ok(try!(JobStatus::from_str(status.as_str())))
    }

    pub fn issue_job(&self, query_type: QueryType, database_name: &str, query: &str,
                     result_url: Option<&str>, 
                     priority: Option<u64>,
                     retry_limit: Option<u64>,
                     domain_key: Option<&str>, 
                     scheduled_time: Option<TimeStamp>) -> Result<u64, TreasureDataError> {
        let mut body = BTreeMap::new();
        body.insert("query".to_string(), query.to_string().to_json());
        result_url.and_then(|x| body.insert("result".to_string(), x.to_string().to_json()));
        priority.and_then(|x| body.insert("priority".to_string(), x.to_string().to_json()));
        retry_limit.and_then(|x| body.insert("retry_limit".to_string(), x.to_string().to_json()));
        domain_key.and_then(|x| body.insert("domain_key".to_string(), x.to_string().to_json()));
        scheduled_time.and_then(|x|
                                body.insert("scheduled_time".to_string(),
                                x.to_string().to_json()));

        let response_body = try!(
            self.get_response_as_string(
                self.http_client.
                    post(format!("{}/v3/job/issue/{}/{}",
                                self.endpoint, query_type.to_string(), database_name).as_str()).
                    header(ContentType(Mime(TopLevel::Application, SubLevel::Json, vec![]))).
                    body(Json::Object(body).to_string().as_str())
            )
        );
        let json: json::Json = try!(json::Json::from_str(response_body.as_str()));
        let job_id = pick_string_item!(json, "job_id");
        job_id.parse::<u64>().
            map_err(|_| 
                    TreasureDataError::JsonDecodeError(
                        DecoderError::ExpectedError("U64".to_string(), job_id)))
    }

    pub fn wait_job(&self, job_id: u64, interval_secs: Option<u64>)
        -> Result<JobStatus, TreasureDataError> {
        let interval_secs = match interval_secs { Some(i) => i, None => 10 };
        loop {
            match self.job_status(job_id) {
                Ok(status) => {
                    match status {
                         JobStatus::Queued | JobStatus::Running => (),
                         _ => return Ok(status)
                    }
                },
                e @ Err(TreasureDataError::JsonDecodeError(_)) => return e,
                Err(_) => ()
            };
            thread::sleep(Duration::from_secs(interval_secs));
        }
    }

    pub fn job_result(&self, job_id: u64)
        -> Result<(Response, usize), TreasureDataError> {
        let response = try!(
            self.get_response(
                self.http_client.
                    get(format!("{}/v3/job/result/{}?format=msgpack_gz",
                                self.endpoint, job_id).as_str())
            )
        );

        let &ContentLength(content_len) = try!(
            response.headers.get::<ContentLength>().
            ok_or(TreasureDataError::ApiError(
                    response.status,
                    "Content-Lentgh doesn't exist".to_string()))
            );
        let content_len = content_len as usize;

        Ok((response, content_len))
    }

    pub fn download_job_result(&self, job_id: u64, out_file: &File)
        -> Result<(), TreasureDataError> {
        let (mut response, content_len) = try!(self.job_result(job_id));

        let mut total_read_len = 0;
        let mut in_buf: [u8; 8192] = [0; 8192];
        let mut out_buf = BufWriter::new(out_file);
        while total_read_len < content_len {
            let read_len = try!(response.read(&mut in_buf));
            total_read_len += read_len;
            try!(out_buf.write(&in_buf[0..read_len]));
        }
        if total_read_len > content_len {
            warn!("content_len={}, total_read_len={}", content_len, total_read_len);
        }
        try!(out_buf.flush());
        Ok(())
    }

    pub fn each_row_in_job_result(&self, job_id: u64, f: &Fn(Vec<Value>))
        -> Result<(), TreasureDataError> {
        let (response, _) = try!(self.job_result(job_id));

        let mut d = try!(GzDecoder::new(response));

        loop {
            match ::msgpack::decode::read_value(&mut d) {
                Ok(Value::Array(xs)) => f(xs),
                Ok(unexpected) => return Err(TreasureDataError::MsgpackUnexpectedValueError(unexpected)),
                // TODO: Should handle it in a smarter way?
                Err(value::Error::InvalidMarkerRead(ReadError::UnexpectedEOF)) => break,
                Err(err) => try!(Err(err))
            }
        }

        Ok(())
    }

    pub fn kill_job(&self, job_id: u64)
                        -> Result<(), TreasureDataError> {
        try!(
            self.get_response_as_string(
                self.http_client.
                    post(format!("{}//v3/job/kill/{}",
                                 self.endpoint, job_id).as_str())
            )
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor, Read, Write};
    use std::time::Duration;
    use std::net::{SocketAddr, Shutdown};
    use hyper::Url;
    use hyper::net::NetworkStream;
    use hyper::client::{RequestBuilder, Response};

    use super::RequestExecutor;
    use client::Client;
    use error::TreasureDataError;

    const APIKEY : &'static str = "1234abcd";

    #[test]
    fn new() {
        let client = Client::new(APIKEY);
        assert_eq!(APIKEY, client.apikey);
        assert_eq!("https://api.treasuredata.com", client.endpoint);
    }

    #[test]
    fn endpoint() {
        let mut client = Client::new(APIKEY);
        client.endpoint("https://foo.com");
        assert_eq!("https://foo.com", client.endpoint);
        client.endpoint("http://bar.com");
        assert_eq!("http://bar.com", client.endpoint);
        client.endpoint("baz.com");
        assert_eq!("https://baz.com", client.endpoint);
    }

    struct MockStream {
        response: Cursor<Vec<u8>>
    }

    impl MockStream {
        fn new(response: Vec<u8>) -> Self {
            MockStream {
                response: Cursor::new(response)
            }
        }
    }

    impl Read for MockStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.response.read(buf)
        }
    }

    impl Write for MockStream {
        fn write(&mut self, msg: &[u8]) -> io::Result<usize> {
            Ok(msg.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl NetworkStream for MockStream {
        fn peer_addr(&mut self) -> io::Result<SocketAddr> {
            Ok("127.0.0.1:55555".parse().unwrap())
        }

        fn set_read_timeout(&self, _: Option<Duration>) -> io::Result<()> {
            Ok(())
        }

        fn set_write_timeout(&self, _: Option<Duration>) -> io::Result<()> {
            Ok(())
        }

        fn close(&mut self, _: Shutdown) -> io::Result<()> {
            Ok(())
        }
    }

    struct MockRequestExecutor {
        response: String
    }

    impl MockRequestExecutor {
        fn new(response_header: Vec<&str>, response_body: &str) -> Self {
            let content_length = format!("Content-Length: {:?}", response_body.len());
            {
                let mut response = response_header.clone();
                response.push(content_length.as_str());
                response.push("");
                response.push(response_body);
                MockRequestExecutor {
                    response: response.join("\r\n").to_string()
                }
            }
        }
    }

    impl RequestExecutor for MockRequestExecutor {
        fn get_response(&self, _: RequestBuilder)
            -> Result<Response, TreasureDataError> {
                Ok(
                    Response::new(
                        Url::parse("https://localhost:55555/foo/bar").unwrap(),
                        Box::new(
                            MockStream::new(self.response.as_str().as_bytes().clone().to_vec())
                        )
                    ).unwrap()
                )
        }
    }

    #[test]
    fn databases() {
        {
            let request_exec = MockRequestExecutor::new(
                vec!["HTTP/1.1 200 OK",
                     "Content-Type: application/json"],
                     "{\"databases\":[]}");
            let client = Client::<MockRequestExecutor>::
                            new_with_request_executor(APIKEY, request_exec);
            let databases = client.databases().unwrap();
            assert_eq!(0, databases.len());
        }

        {
            let request_exec = MockRequestExecutor::new(
                vec!["HTTP/1.1 200 OK",
                     "Content-Type: application/json"],
                     r#"{"databases":[
                       {"name":"db0", "count":42, "created_at":"2016-01-01 00:00:00 UTC",
                        "updated_at":"2016-01-01 01:01:01 UTC", "permission":"query_only"},
                       {"name":"db1", "count":0, "created_at":"2016-12-31 23:59:59 UTC",
                        "updated_at":"2016-12-31 23:59:59 UTC", "permission":"administrator"}
                     ]}"#
            );
            let client = Client::<MockRequestExecutor>::
                            new_with_request_executor(APIKEY, request_exec);
            let databases = client.databases().unwrap();
            assert_eq!(2, databases.len());

            let db0 = databases.get(0).unwrap();
            assert_eq!("db0", db0.name);
            assert_eq!(42, db0.count);
            assert_eq!("2016-01-01 00:00:00 UTC", db0.created_at.to_string());
            assert_eq!("2016-01-01 01:01:01 UTC", db0.updated_at.to_string());
            assert_eq!("query_only", db0.permission);

            let db1 = databases.get(1).unwrap();
            assert_eq!("db1", db1.name);
            assert_eq!(0, db1.count);
            assert_eq!("2016-12-31 23:59:59 UTC", db1.created_at.to_string());
            assert_eq!("2016-12-31 23:59:59 UTC", db1.updated_at.to_string());
            assert_eq!("administrator", db1.permission);
        }
    }
}
