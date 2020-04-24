extern crate rand;
extern crate td_client;

use std::env;
use rand::Rng;

use td_client::client::*;
use td_client::model::*;

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

    if client.databases().unwrap().iter().all(|db| db.name != database) {
        client.create_database(database.as_str()).unwrap();
    }
}

