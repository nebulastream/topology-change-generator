use serde_with::DurationMilliSeconds;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::{fs, time};
use std::time::Duration;
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

#[derive(Debug, Serialize, Deserialize)]
#[derive(Clone)]
struct MobileEntry {
    device_id: u64,
    sources: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MobileDeviceQuadrants {
    quadrant_map: BTreeMap<u64, VecDeque<MobileEntry>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QuadrantConfig {
    pub num_quadrants: usize,
    pub devices_per_quadrant: usize,
    pub quadrant_start_id: u64,
    pub mobile_start_id: u64,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum ISQPEventAction {
    add,
    remove,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ISQPEvent {
    #[serde(rename = "parentId")]
    pub parent_id: u64,
    #[serde(rename = "childId")]
    pub child_id: u64,
    pub action: ISQPEventAction,
}


#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TopologyUpdate {
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    pub timestamp: time::Duration,
    #[serde(rename = "events")]
    pub events: Vec<ISQPEvent>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FixedTopology {
    pub nodes: HashMap<u64, Vec<f64>>,
    pub slots: HashMap<u64, u16>,
    pub children: HashMap<u64, Vec<u64>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SimulatedReconnects {
    initial_parents: Vec<(u64, u64)>,
    pub topology_updates: Vec<TopologyUpdate>,
}

impl SimulatedReconnects {
    fn from_mobile_device_quadrants(mdq: MobileDeviceQuadrants, runtime: Duration, interval: Duration, num_of_devices_to_rotate: u16) -> Self {
        Self {
            initial_parents: mdq.get_initial_update(),
            topology_updates: mdq.get_update_vector(runtime, interval, num_of_devices_to_rotate),
        }
    }
}

impl FixedTopology {
    fn create_single_fog_layer_topology_with_default_location(num_nodes: usize, default_resources: u16) -> Self {
        let mut nodes = HashMap::new();
        let mut slots = HashMap::new();
        let mut children = HashMap::new();
        for i in 0..num_nodes {
            nodes.insert(i as u64, vec![0.0, 0.0]);
            slots.insert(i as u64, default_resources);
            children.insert(i as u64, vec![]);
        }
        Self {
            nodes,
            slots,
            children,
        }
    }
}

impl From<QuadrantConfig> for FixedTopology {
    fn from(config: QuadrantConfig) -> Self {
        Self::create_single_fog_layer_topology_with_default_location(config.num_quadrants, 65535)
    }
}

impl FixedTopology{
    fn from(mdq: MobileDeviceQuadrants, subtract: u64) -> Self {
        let mut nodes = HashMap::new();
        let mut slots = HashMap::new();
        let mut children = HashMap::new();
        for (quadrant_id, _) in mdq.quadrant_map.iter() {
            nodes.insert(*quadrant_id - subtract, vec![0.0, 0.0]);
            slots.insert(*quadrant_id - subtract, 65535);
            children.insert(*quadrant_id - subtract, vec![]);
        }
        Self {
            nodes,
            slots,
            children,
        }
    }
}

impl MobileDeviceQuadrants {
    fn rotate_devices(&mut self, num_devices: u16) -> Vec<ISQPEvent> {
        let mut events = vec![];
        let mut moving_devices: Vec<Option<(u64, MobileEntry)>> = vec![];
        for (quadrant_id, devices) in self.quadrant_map.iter_mut().rev() {
            //for (quadrant_id, devices) in self.quadrant_map.iter_mut() {
            for mut moving_device in &mut moving_devices {
                Self::rotate_single_device(&mut events, &mut moving_device, *quadrant_id, devices);
            }
            for _ in 0..num_devices {
                if let Some(device) = devices.pop_front() {
                    moving_devices.push(Some((*quadrant_id, device)));
                }
            }
        }
        let mut entry = self.quadrant_map.last_entry().unwrap();
        for mut moving_device in &mut moving_devices {
            Self::rotate_single_device(&mut events, &mut moving_device, *entry.key(), entry.get_mut());
        }
        events
    }

    fn rotate_single_device(events: &mut Vec<ISQPEvent>, moving_device: &mut Option<(u64, MobileEntry)>, quadrant_id: u64, devices: &mut VecDeque<MobileEntry>) {
        if let Some((old_quadrant, device)) = moving_device.take() {
            events.push(
                ISQPEvent {
                    parent_id: old_quadrant,
                    child_id: device.device_id,
                    action: ISQPEventAction::remove,
                }
            );
            events.push(
                ISQPEvent {
                    parent_id: quadrant_id,
                    child_id: device.device_id,
                    action: ISQPEventAction::add,
                }
            );
            devices.push_back(device);
        }
    }

    fn new() -> Self {
        Self {
            quadrant_map: BTreeMap::new()
        }
    }

    fn populate(num_quadrants: usize, devices_per_qudrant: usize, quadrant_start_id: u64, mobile_start_id: u64) -> Self {
        assert!(quadrant_start_id + num_quadrants as u64 - 1 < mobile_start_id);
        let mut quadrant_map = BTreeMap::new();
        for i in 0..num_quadrants {
            let mut devices = VecDeque::new();
            for j in 0..devices_per_qudrant {
                //devices.push((mobile_start_id + i as u64 * devices_per_qudrant as u64 + j as u64, vec![]));
                devices.push_back(MobileEntry {
                    device_id: mobile_start_id + i as u64 * devices_per_qudrant as u64 + j as u64,
                    sources: vec![],
                });
            }
            quadrant_map.insert(quadrant_start_id + i as u64, devices);
        };
        Self {
            quadrant_map
        }
    }
    pub fn get_update_vector(mut self, runtime: Duration, interval: Duration, num_devices_to_rotate: u16) -> Vec<TopologyUpdate> {
        let mut updates = vec![];

        let mut timestamp = Duration::new(0, 0);

        //insert reconnects
        while timestamp < runtime {
            updates.push(TopologyUpdate {
                timestamp,
                events: self.rotate_devices(num_devices_to_rotate),
            });
            timestamp += interval;
        }
        updates
    }

    pub fn get_initial_update(&self) -> Vec<(u64, u64)> {
        let mut changes = vec![];
        for (quadrant_id, devices) in self.quadrant_map.iter() {
            for device in devices {
                changes.push((*quadrant_id, device.device_id));
            }
        }
        changes
    }

    pub fn compute_source_groups(&self, subtract: u64) -> HashMap<u64, Vec<u64>> {
        let mut source_groups = HashMap::new();
        for (quadrant_id, devices) in self.quadrant_map.iter() {
            for device in devices {
                source_groups.insert(device.device_id, vec![*quadrant_id - subtract]);
            }
        }
        source_groups
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;
    use crate::FixedTopology;


    #[test]
    fn test_json_output() {
        let mut mdq = super::MobileDeviceQuadrants::populate(4, 3, 1, 100);
        let json = serde_json::to_string_pretty(&mdq).unwrap();
        println!("{}", json);
        let isqp_events = mdq.rotate_devices(2);
        let json = serde_json::to_string_pretty(&isqp_events).unwrap();
        println!("{}", json);
        let json = serde_json::to_string_pretty(&mdq).unwrap();
        println!("{}", json);
    }

    #[test]
    fn test_artifical_data_generation() {
        let mdq = super::MobileDeviceQuadrants::populate(4, 4, 1, 100);
        let topology = FixedTopology::from(mdq.clone(), 1);
        let json = serde_json::to_string_pretty(&topology).unwrap();
        println!("{}", json);
        let source_groups = mdq.compute_source_groups(1);
        let json = serde_json::to_string_pretty(&source_groups).unwrap();
        println!("{}", json);
        let _ = serde_json::to_string_pretty(&mdq).unwrap();
        let simulated_reconnects = super::SimulatedReconnects::from_mobile_device_quadrants(mdq, std::time::Duration::new(6, 0), std::time::Duration::new(2, 0), 2);
        let json = serde_json::to_string_pretty(&simulated_reconnects).unwrap();
        println!("{}", json);
    }

    #[test]
    fn test_list() {
        let mut mdq = super::MobileDeviceQuadrants::populate(4, 4, 1, 100);
        let json = serde_json::to_string_pretty(&mdq).unwrap();
        println!("{}", json);
        let isqp_events = mdq.get_update_vector(std::time::Duration::new(6, 0), std::time::Duration::new(2, 0), 2);
        let json = serde_json::to_string_pretty(&isqp_events).unwrap();
        println!("{}", json);
    }

    #[test]
    fn test_time() {
        let now = SystemTime::now();
        let epoch_now = now.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        println!("{:?}", epoch_now);
        println!("{:?}", now);

    }
}

/// Program to generate synthetic topology change events
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Output path for the generated files
    #[arg(short, long, default_value = ".")]
    output_path: String,

    /// Number of fog nodes in the generated topology
    #[arg(short, long, default_value_t = 10)]
    fog_nodes: u64,

    /// Number of mobile devices to be created for each fog node
    #[arg(short, long, default_value_t = 10)]
    mobile_devices_per_fog_node: u64,

    /// Number of moving devices per topology update
    #[arg(long, default_value_t = 1)]
    moving_devices: u16,
}

fn main() {
    let args = Args::parse();
    
    let output_path = args.output_path;
    let quadrants = args.fog_nodes;
    let quadrant_start_id = 2;
    let mobile_devices_per_quadrant = args.mobile_devices_per_fog_node;
    let num_of_devices_to_rotate = args.moving_devices;
    let mdq = MobileDeviceQuadrants::populate(
        quadrants as usize,
        mobile_devices_per_quadrant as usize,
        quadrant_start_id,
        (quadrants) as u64 + quadrant_start_id
    );

    /*subtract 1 from id because the runner script expects the ids to start at 1
     * but the reconnects are generated with the ids starting at 2 (coordinator has 1 as id)
     */
    let topology = FixedTopology::from(mdq.clone(), 1);
    let json = serde_json::to_string_pretty(&topology).unwrap();
    let topology_output_path = format!("{}/fixed_topology.json", output_path);
    fs::write(topology_output_path, json).unwrap();


    let source_groups = mdq.compute_source_groups(1);
    let json = serde_json::to_string_pretty(&source_groups).unwrap();
    let source_groups_output_path = format!("{}/source_groups.json", output_path);
    fs::write(source_groups_output_path, json).unwrap();

    let runtime = std::time::Duration::new(120, 0);
    let interval = std::time::Duration::from_millis(1000);
    let simulated_reconnects = SimulatedReconnects::from_mobile_device_quadrants(mdq, runtime, interval, num_of_devices_to_rotate);
    let json = serde_json::to_string_pretty(&simulated_reconnects).unwrap();
    let simulated_reconnects_output_path = format!("{}/topology_updates.json", output_path);
    fs::write(simulated_reconnects_output_path, json).unwrap();
}
