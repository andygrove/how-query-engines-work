extern crate clap;
use clap::{Arg, App, SubCommand, ArgMatches};
use std::time::Instant;

use ballista::cluster;

pub fn main() {
    let _ = ::env_logger::init();

    let now = Instant::now();

    let matches = App::new("Ballista")
        .version("0.1.0")
        .author("Andy Grove <andygrove73@gmail.com>")
        .about("Distributed compute platform")
        .subcommand(SubCommand::with_name("create-cluster")
            .about("Create a ballista cluster")
            .arg(Arg::with_name("name")
                .required(true)
                .takes_value(true)
                .short("n")
                .help("Ballista cluster name"))
            .arg(Arg::with_name("executors")
                .short("e")
                .required(true)
                .takes_value(true)
                .help("number of executor pods to create")))
        .subcommand(SubCommand::with_name("delete-cluster")
            .about("Delete a ballista cluster")
            .arg(Arg::with_name("name")
                .required(true)
                .takes_value(true)
                .short("n")
                .help("Ballista cluster name")))
        .get_matches();

    match matches.subcommand() {
        ("create-cluster", Some(subcommand_matches)) => create_cluster(subcommand_matches),
        ("delete-cluster", Some(subcommand_matches)) => delete_cluster(subcommand_matches),
        _ => {
            println!("Invalid subcommand");
            std::process::exit(-1);
        }
    }

    println!("Executed subcommand {} in {} seconds", matches.subcommand_name().unwrap(), now.elapsed().as_millis() as f64 / 1000.0);
}

fn create_cluster(matches: &ArgMatches) {
    let cluster_name = matches.value_of("name").unwrap();
    let exec_node_count= matches.value_of("executors").unwrap().parse::<usize>().unwrap();

    // create a cluster with 12 pods (one per month)
    for i in 1..=exec_node_count {
        let pod_name = format!("ballista-{}-{}", cluster_name, i);
        cluster::create_ballista_pod(&pod_name).unwrap();
    }
}

fn delete_cluster(matches: &ArgMatches) {
    let cluster_name = matches.value_of("name").unwrap();

    unimplemented!();

}
