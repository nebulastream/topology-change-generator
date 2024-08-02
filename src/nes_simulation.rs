use std::collections::{BTreeMap, HashMap};
use std::{fs, time};
use std::collections::btree_map::Entry;
use std::ops::Sub;
use std::time::Duration;
// use polars::export::chrono::Duration;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use crate::cell_data::{MultiTripAndCellData, RadioCell};
use serde_with::DurationMilliSeconds;

#[derive(Serialize, Deserialize, Debug)]
pub struct FixedTopology {
    //todo: check if we can just make that a tuple
    pub nodes: HashMap<u64, Vec<f64>>,
    pub slots: HashMap<u64, u16>,
    pub children: HashMap<u64, Vec<u64>>,
}


#[serde_as]
#[derive(Serialize, Deserialize, Clone)]
pub struct TopologyUpdate {
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    pub timestamp: time::Duration,
    #[serde(rename = "events")]
    pub events: Vec<ISQPEvent>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ISQPEvent {
    #[serde(rename = "parentId")]
    pub parent_id: u64,
    #[serde(rename = "childId")]
    pub child_id: u64,
    pub action: ISQPEventAction,
}

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub enum ISQPEventAction {
    add,
    remove,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SimulatedReconnects {
    initial_parents: Vec<(u64, u64)>,
    topology_updates: Vec<TopologyUpdate>,
}

impl SimulatedReconnects {
    pub fn from_topology_and_cell_data(topology: FixedTopology, cell_data: MultiTripAndCellData, cell_id_to_node_id: HashMap<(u64, u64), u64>, start_time: Duration) -> SimulatedReconnects {
        let mut initial_parents = vec![];
        let mut topology_update_map = BTreeMap::new();
        let mut child_id = topology.nodes.keys().max().unwrap() + 1;
        let mut reconnect_count = 0;
        for trip in cell_data.trips {
            let point = trip.trip.shape_points.first().unwrap();
            let shape_id = point.shape_id.clone();
            let sequence_nr = point.shape_pt_sequence;

            let cell_id = trip.cell_data.get(&(shape_id, sequence_nr)).unwrap();
            let mut parent_id = *cell_id_to_node_id.get(cell_id).unwrap();
            initial_parents.push((child_id, parent_id));
            let mut previous_parent_id = parent_id;
            // dbg!(&trip.trip.shape_points);
            for point in &trip.trip.shape_points[1..] {
                let shape_id = point.shape_id.clone();
                let sequence_nr = point.shape_pt_sequence;

                let cell_id = trip.cell_data.get(&(shape_id, sequence_nr)).unwrap();
                parent_id = *cell_id_to_node_id.get(cell_id).unwrap();
                
                if parent_id != previous_parent_id {
                    reconnect_count += 1;
                    //create topology update
                    let timestamp = point.time.expect("No time set for shape point").sub(start_time);
                    // let update_at_time = match topology_update_map.entry(timestamp) {
                    //     Entry::Occupied(e) => e.into_mut(),
                    //     Entry::Vacant(e) => e.insert(TopologyUpdate { timestamp, events: vec![] })
                    // };

                    let update_at_time = topology_update_map.entry(timestamp).or_insert(TopologyUpdate { timestamp, events: vec![] });

                    update_at_time.events.push(ISQPEvent {
                        parent_id: previous_parent_id,
                        child_id,
                        action: ISQPEventAction::remove,
                    });

                    update_at_time.events.push(ISQPEvent {
                        parent_id,
                        child_id,
                        action: ISQPEventAction::add,
                    });
                }



                previous_parent_id = parent_id;
            }
            child_id += 1;
        }
        println!("Total Reconnects: {}", reconnect_count, );
        SimulatedReconnects { initial_parents, topology_updates: topology_update_map.into_values().collect() }
    }
}

impl FixedTopology {
    pub fn write_to_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        // let json_string = std::fs::read_to_string(&self.paths.get_fixed_topology_nodes_path())?;
        // let topology: FixedTopology = serde_json::from_str(json_string.as_str())?;
        let json_string = serde_json::to_string_pretty(self)?;
        Ok(fs::write(path, json_string)?)
    }

    fn create_single_fog_layer(start_id: u64, default_resoucres: u16, radio_cells: &Vec<&RadioCell>) -> (FixedTopology, HashMap<(u64, u64), u64>) {
        let mut nodes = HashMap::new();
        let mut slots = HashMap::new();
        let mut children = HashMap::new();
        let mut cell_id_to_node_id = HashMap::new();
        for (i, cell) in radio_cells.iter().enumerate() {
            let id = i as u64 + start_id;
            nodes.insert(id, vec![cell.lon, cell.lat]);
            slots.insert(id, default_resoucres);
            children.insert(id, vec![]);
            cell_id_to_node_id.insert((cell.id, cell.mnc), id);
        }
        (FixedTopology { nodes, slots, children }, cell_id_to_node_id)
    }
}

pub fn create_single_fog_layer_topology_from_cell_data(start_id: u64, default_resources: u16, cell_data: &MultiTripAndCellData) -> (FixedTopology, HashMap<(u64, u64), u64>) {
    FixedTopology::create_single_fog_layer(start_id, default_resources, &cell_data.radio_cells.values().collect())
}

