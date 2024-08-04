use std::collections::{HashMap, HashSet};
use rusqlite::{Connection, named_params, params, Result};
use geo::VincentyDistance;
use simulation_curator::gtfs;
use simulation_curator::gtfs::ShapePoint;
use simulation_curator::cell_data;
use simulation_curator::nes_simulation;
use simulation_curator::nes_simulation::create_single_fog_layer_topology_from_cell_data;
use clap::Parser;

/// Program to generate topology change events
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the gtfs database
    #[arg(short, long, default_value = "gtfs_vbb.db")]
    dbPath: String,

    ///  The time of the day from when the schedule needs to be selected
    #[arg(short, long, default_value = "08:00:00")]
    start_time: String,

    /// The time of the day till when the schedule needs to be selected
    #[arg(short, long, default_value = "09:00:00")]
    end_time: String,

    /// The day of the week for which the schedule needs to be selected
    #[arg(short, long, default_value = "monday")]
    day: String,

    /// The time interval in seconds that need to be represented by one second. This parameter allows us to speedup the time to increase the rate of topology changes.
    #[arg(short, long, default_value_t = 20)]
    batch_interval_size_in_seconds: u64,

    /// The frequency at which the topology changes need to be produced
    #[arg(short, long, default_value_t = 4)]
    change_frequency_in_seconds: u64,

    /// Path to the file where fixed_topology.json will be produced
    #[arg(short, long, default_value = "fixed_topology.json")]    
    topology_path: String,

    /// Path to the file where topology_updates.json will be produced
    #[arg(short, long, default_value = "topology_updates.json")]
    topology_updates_path: String, 

    /// Path to the file where geo.json will be produced
    #[arg(short, long, default_value = "geo.json")]
    geo_json_path: String,

    /// Name of the csv file containing OpenCelliD data.
    #[arg(short, long, default_value = "OpenCelliDGermanyData.csv")]
    open_cell_id_data_loc: String,

    /// The number of people connected to the base station.
    #[arg(short, long, default_value_t = 10)]
    min_samples: u64,

    /// The type of network the base station support. We use LTE only for our experiments.
    #[arg(short, long, default_value = "LTE")]
    radio: String,
    
    
}

fn main() -> Result<()> {
    let args = Args::parse();

    //db
    let db = Connection::open(args.dbPath)?;

    //time window
    let start_time = gtfs::parse_duration(&(args.start_time)).unwrap();
    let end_time = gtfs::parse_duration(&(args.end_time)).unwrap();

    //cell id params
    let network_id = vec![2, 4, 9]; // For vodafone
    let beginning_of_2024 = 1704067200;

    // NebulaStream related config
    let start_node_id = 2;

    // get routes and trips for a specific calender date
    let mut stmt = db.prepare("SELECT DISTINCT block_id \
                                             FROM routes, trips, calendar_dates \
                                             WHERE trips.route_id=routes.route_id \
                                                    AND trips.service_id=calendar_dates.service_id\
                                                    AND routes.route_short_name in ('S41') \
                                                    AND calendar_dates.date='2024-07-29'")?;

    let block_ids = stmt.query_map(params![], |row| {
        Ok(row.get::<usize, String>(0))
    })?;

    let mut partial_trips = Vec::new();
    // let mut all_shape_points = HashSet::new();
    for block_id in block_ids {
        if let Some(trip) = gtfs::read_stops_for_trip(block_id.unwrap().unwrap(), &db, start_time, end_time).unwrap() {
            partial_trips.push(trip);
        }
    }
    
    println!("partial trips: {}", partial_trips.len());
    let cells = cell_data::get_closest_cells_from_csv(&(args.open_cell_id_data_loc), &(args.radio), 262, &network_id, 0.0, 0.0, 0, beginning_of_2024, args.min_samples, partial_trips);
    println!("cell towers {}", cells.radio_cells.len());
    let gj = cells.to_geojson();

    std::fs::write(args.geo_json_path, gj.to_string()).unwrap();

    //set default resources to max value
    let default_resources = u16::MAX;
    //create a topology and write it to json
    let (topology, cell_id_to_node_id) = create_single_fog_layer_topology_from_cell_data(2, default_resources, &cells);
    topology.write_to_file(&(args.topology_path)).unwrap();

    let batch_interval = std::time::Duration::from_secs(args.batch_interval_size_in_seconds);
    let batch_gap = std::time::Duration::from_secs(args.change_frequency_in_seconds);
    let simulated_reconnects = nes_simulation::SimulatedReconnects::from_topology_and_cell_data(topology, cells, cell_id_to_node_id, start_time, batch_interval.into(), batch_gap.into());
    println!("topology updates: {}", simulated_reconnects.topology_updates.len());
    let json_string = serde_json::to_string_pretty(&simulated_reconnects).unwrap();
    std::fs::write(args.topology_updates_path, json_string).unwrap();

    Ok(())
}
