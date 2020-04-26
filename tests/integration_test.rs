extern crate rand;
extern crate td_client;

use std::env;
use rand::Rng;

use td_client::client::*;
use td_client::error::*;
use td_client::model::*;

fn test_with_database(client: &Client<DefaultRequestExecutor>, database: &str) -> Result<(), TreasureDataError> {
    if client.databases()?.iter().all(|db| db.name != database) {
        client.create_database(database)?;
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

    let mut client = Client::new(apikey.as_str());
    client.endpoint("https://api-development.treasuredata.com");

    let mut rng = rand::thread_rng();

    let database = {
        let mut s = String::from("td_client_rust_db_");
        let r: u16 = rng.gen();
        s.push_str(r.to_string().as_str());
        s
    };

    let result = test_with_database(&client, &database);
    if client.databases().unwrap().iter().any(|db| db.name == database) {
        client.delete_database(&database).unwrap();
    }

    assert!(result.is_ok());
}

