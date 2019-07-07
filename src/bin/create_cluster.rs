use std::time::Instant;

use ballista::cluster;

pub fn main() {
    let _ = ::env_logger::init();

    //TODO cmd line args
    let cluster_name = "ballista";
    let exec_node_count= 1;

    let now = Instant::now();

    // create a cluster with 12 pods (one per month)
    for month in 1..=exec_node_count {
        let pod_name = format!("{}-{}", cluster_name, month);
        cluster::create_ballista_pod(&pod_name).unwrap();
    }

    println!("Created ballista cluster containing {} executors in {} seconds", exec_node_count, now.elapsed().as_millis() as f64 / 1000.0);
}
