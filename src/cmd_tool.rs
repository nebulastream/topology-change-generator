use std::collections::{HashMap, HashSet};
use rusqlite::{Connection, named_params, params, Result};
use geo::VincentyDistance;
use simulation_curator::gtfs;
use simulation_curator::gtfs::ShapePoint;
use simulation_curator::cell_data;
use simulation_curator::nes_simulation;
use simulation_curator::nes_simulation::create_single_fog_layer_topology_from_cell_data;

fn main() -> Result<()> {
    //todo: take command line args
    
    //db
    let dbPath = "gtfs_vbb.db";
    let db = Connection::open(dbPath)?;
    
    //time window
    let start_time = gtfs::parse_duration("08:00:00").unwrap();
    let end_time = gtfs::parse_duration("09:00:00").unwrap();
    let day = "monday";
    let batchIntervalSizeInSeconds = 20;
    let changeFrequencyInSeconds = 4;
    
    //output paths
    let topology_path = "fixed_topology.json";
    let topology_updates_path = "topology_updates.json";
    let geo_json_path = "geo.json";
    
    //cell id params
    let file_path  = "OpenCelliDGermanyData.csv";
    let networkIdentifier = vec![2, 4, 9]; // For vodafone
    let beginning_of_2024 = 1704067200;
    let min_samples = 10;
    let radio = "LTE";

    // NebulaStream related config
    let start_node_id = 2;

    // get any trip id from the line S3 and print all the stop names in order of the trip

    // let mut stmt = db.prepare("SELECT stop_name arrival_time FROM stops WHERE stop_id IN (SELECT stop_id FROM stop_times WHERE trip_id IN (SELECT trip_id FROM trips WHERE route_id IN (SELECT route_id FROM routes WHERE route_short_name='S3') LIMIT 1)) ORDER BY arrival_time")?;
    // let stop_names = stmt.query_map(params![], |row| {
    //     Ok(row.get::<usize, String>(0))
    // })?;



    // let mut stmt = db.prepare("SELECT route_id FROM routes WHERE route_short_name IN ('S41', 'S42')")?;
    // let mut stmt = db.prepare("SELECT route_id FROM routes WHERE route_short_name IN ('S41')")?;
    let mut stmt = db.prepare("SELECT DISTINCT block_id FROM routes, trips, calendar_dates WHERE trips.route_id=routes.route_id AND trips.service_id=calendar_dates.service_id and routes.route_short_name in ('S41') and calendar_dates.date='2024-07-29'")?;
    // let mut stmt = db.prepare("SELECT route_id FROM routes WHERE route_short_name IN ('S3')")?;
    // let mut stmt = db.prepare("SELECT route_id FROM routes")?;
    let block_ids = stmt.query_map(params![], |row| {
        Ok(row.get::<usize, String>(0))
    })?;

    let mut partial_trips = Vec::new();
    // let mut all_shape_points = HashSet::new();
    for block_id in block_ids {
        // let id = block_id.unwrap().unwrap();
        //
        // //retrieve trips
        // let mut stmt = db.prepare("SELECT route_id, trip_id, service_id FROM trips WHERE route_id=:route_id")?;
        // let trips = stmt.query_map(named_params! {":route_id": id}, |row| {
        //     Ok((row.get::<usize, String>(0), row.get::<usize, String>(1), row.get::<usize, String>(2)))
        // })?;
        //
        // for trip in trips {
        //
        //
        //     //read stops and print geojson
        //     let (route_id, trip_id, service_id) = trip.unwrap();

            if let Some(trip) = gtfs::read_stops_for_trip(block_id.unwrap().unwrap(), &db, start_time, end_time).unwrap() {
                partial_trips.push(trip);
            }


        // }
    }
    //todo: move the piecing together logic to a separate lib file
    
    //tddo: use or remove coordinates as arguments
    println!("partial trips: {}", partial_trips.len());
    let cells = cell_data::get_closest_cells_from_csv(file_path, radio, 262, &networkIdentifier, 0.0, 0.0, 0, beginning_of_2024, min_samples, partial_trips);
    println!("cell towers {}", cells.radio_cells.len());
    // let gj = gtfs::to_geojson(partial_trips);
    let gj = cells.to_geojson();

    std::fs::write(geo_json_path, gj.to_string()).unwrap();

    //set default resources to max value
    let default_resources = u16::MAX;
    //create a topology and write it to json
    let (topology, cell_id_to_node_id) = create_single_fog_layer_topology_from_cell_data(2, default_resources, &cells);
    topology.write_to_file(topology_path).unwrap();

    let batch_interval = std::time::Duration::from_secs(batchIntervalSizeInSeconds);
    let batch_gap = std::time::Duration::from_secs(changeFrequencyInSeconds);
    let simulated_reconnects = nes_simulation::SimulatedReconnects::from_topology_and_cell_data(topology, cells, cell_id_to_node_id, start_time, batch_interval.into(), batch_gap.into());
    // let simulated_reconnects = nes_simulation::SimulatedReconnects::from_topology_and_cell_data(topology, cells, cell_id_to_node_id, start_time, None, None);
    println!("topology updates: {}", simulated_reconnects.topology_updates.len());
    let json_string = serde_json::to_string_pretty(&simulated_reconnects).unwrap();
    std::fs::write(topology_updates_path, json_string).unwrap();

    Ok(())
}
