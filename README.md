# td-client-rust
[<img src="https://travis-ci.org/komamitsu/td-client-rust.svg?branch=master"/>](https://travis-ci.org/komamitsu/td-client-rust)
Rust Client Library for Treasure Data

## Install

Add the following dependency in Cargo.toml

```
[dependencies]
td-client = "0.1"
```

## Usage


First, add this line to your crate root

```rust
extern crate td_client;
```

Next, set up a td-client with your API key for Treasure Data

```rust
use td_client::client::*;
use td_client::model::*;

let client = Client::new("your API key");
```

### Manipulate Database metadata

```rust
client.create_database("my_database").unwrap();
println!("{:?}", client.databases().unwrap());
client.delete_database("unused_database").unwrap();
```

### Manipulate Table metadata

```rust
client.create_table("my_database", "my_table").unwrap();
println!("{:?}", client.tables("my_database").unwrap());
client.swap_table("my_database", "my_table", "my_temp_table").unwrap();
client.rename_table("my_database", "my_temp_table", "unused_table").unwrap();
client.delete_table("my_database", "unused_table").unwrap();
```

### Import data to table

```rust
// Import msgpack gzipped file
client.import_msgpack_gz_file_to_table("my_database", "my_table",
                        "/tmp/2016-08-01.msgpack.gz", None).unwrap();

// Import records
let mut chunk = TableImportWritableChunk::new().unwrap();
chunk.next_row(4).unwrap();
chunk.write_key_and_i64("time", time::get_time().sec).unwrap();
chunk.write_key_and_str("name", "foo").unwrap();
chunk.write_key_and_u8("age", 42).unwrap();
chunk.write_key_and_f32("pi", 3.14).unwrap();

chunk.next_row(3).unwrap();
chunk.write_key_and_i64("time", time::get_time().sec).unwrap();
chunk.write_key_and_str("name", "bar").unwrap();
chunk.write_key_and_u8("age", 56).unwrap();

let readable_chunk = chunk.close().unwrap();

client.import_msgpack_gz_file_to_table("my_database", "my_table", 
			readable_chunk.file_path.as_str(), None).unwrap();
```

### Information of jobs

```rust
// List up jobs
println!("{:?}", client.jobs(Some(JobStatusOption::Success), None, None).unwrap());

// Look at the job
println!("{:?}", client.job(1234567).unwrap());

// Check the job's status
println!("{:?}", client.job_status(1234567).unwrap());
```

### Issue a query

```rust
// Issue a query
let job_id = client.issue_job(
	QueryType::Presto, "sample_datasets",
	"select code, method, count(1) as cnt from www_access group by code, method",
	None, None, None, None, None).unwrap();

println!("job_id={}, status={:?}", job_id, client.wait_job(job_id, None).unwrap());

// Download the result to a file
let result_file = File::create("/tmp/result.msgpack.gz").unwrap();
client.download_job_result(job_id, &result_file).unwrap();

// Do something for each record
client.each_row_in_job_result(job_id, &|xs| println!(">>>> {:?}", xs));
```

