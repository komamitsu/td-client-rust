extern crate rand;
extern crate td_client;

use std::convert::TryInto;
use std::env;
use std::error::Error;
use std::thread;
use std::sync::{Arc, Mutex};
use std::time;
use std::time::SystemTime;
use std::vec::Vec;
use rand::Rng;

use td_client::client::*;
use td_client::model::*;
use td_client::table_import::*;
use td_client::value::*;

fn test_with_database(client: &Client<DefaultRequestExecutor>, database: &str) -> Result<(), Box<dyn Error>> {
    // Prepare database
    if client.databases()?.iter().any(|db| db.name == database) {
        client.delete_database(database)?;
    }
    client.create_database(database)?;

    // Prepare table
    let table = {
        let mut s = String::from("tbl_");
        let r: u16 = rand::thread_rng().gen();
        s.push_str(r.to_string().as_str());
        s
    };

    if client.tables(database)?.iter().any(|tbl| tbl.name == table) {
        client.delete_table(database, &table)?;
    }
    client.create_table(database, &table)?;

    // Import records
    let mut chunk = TableImportWritableChunk::new().unwrap();
    let now: i64 = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs().try_into().unwrap(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!")
    };

    chunk.next_row(4).unwrap();
    chunk.write_key_and_i64("time", now).unwrap();
    chunk.write_key_and_str("name", "foo").unwrap();
    chunk.write_key_and_u8("age", 42).unwrap();
    chunk.write_key_and_f32("pi", 3.14).unwrap();

    chunk.next_row(3).unwrap();
    chunk.write_key_and_i64("time", now + 1).unwrap();
    chunk.write_key_and_str("name", "bar").unwrap();
    chunk.write_key_and_u8("age", 56).unwrap();

    let readable_chunk = chunk.close().unwrap();

    client.import_msgpack_gz_file_to_table(
        database, &table, readable_chunk.file_path.as_str(), None)?;

    // Confirm the imported records
    let mut count = 0;
    loop {
        if count >= 10 {
            Err("Retried over. Imported records are still unvisible")?;
        }

        let job_id = client.issue_job(
            QueryType::Presto, database,
            format!("select count(1) as cnt from {}", table).as_str(),
            None, None, None, None, None)?;

        println!("Waiting the job: {}", job_id);

        client.wait_job(job_id, None)?;

        println!("The job finished: {}", job_id);

        let records = Arc::new(Mutex::new(Vec::new()));
        client.each_row_in_job_result(
            job_id,
            &|xs| {
                records.lock().unwrap().push(xs);
                true
            })?;

        let expected = &Value::Integer(Integer::I64(2));
        let actual = &records.lock().unwrap()[0][0];
        if expected == actual {
            break;
        }
        else if expected < actual {
            Err(format!("Imported records are unexpectedly too many. expected={:?}, actual={:?}", expected, actual))?;
        }

        println!("Imported records are still unvisible. Retrying...");
        thread::sleep(time::Duration::from_secs(30));
        count += 1;
    }

    Ok(())
}

#[test]
fn integration_test() {
    // Run integration tests only when the environment variable is set
    let apikey = match env::var("TD_APIKEY") {
        Ok(x) => x,
        _ => return
    };
    println!("envvar TD_APIKEY is set. Starting the integration test");

    let mut client = Client::new(apikey.as_str());
    client.endpoint("https://api-development.treasuredata.com");

    let database = {
        let mut s = String::from("td_client_rust_db_");
        let r: u16 = rand::thread_rng().gen();
        s.push_str(r.to_string().as_str());
        s
    };
    println!("This integration test will be executed in database `{}`", database);

    let result = test_with_database(&client, &database);
    if client.databases().unwrap().iter().any(|db| db.name == database) {
        client.delete_database(&database).unwrap();
    }

    result.unwrap();
}

