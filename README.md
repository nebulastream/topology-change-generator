# ISQP-TopologyChangeGenerator
### This repository contains code to produce a collection of topology changes based on [OpenCelliD](https://opencellid.org/) database and GTFS schedule of [VBB](https://www.vbb.de/vbb-services/api-open-data/datensaetze/).

# Pre-Processing Steps

1. Download the GTFS schedule from VBB link provided above. The schedule information is used for finding the trajectory of mobile devices.
2. Create a SQLite database from the gtfs schedule using the tool [gtfsdb tool](https://github.com/OpenTransitTools/gtfsdb) using the bellow commands and place the database at the root of the project.
```  
   pip install zc.buildout
   git clone https://github.com/OpenTransitTools/gtfsdb.git
   cd gtfsdb/
   buildout install prod
   bin/gtfsdb-load --database_url sqlite:///vbb_gtfs.db <location of gtfs file>/GTFS.zip
```   
3. Install Rust toolchain (`curl --proto '=https' --tlsv1.3 https://sh.rustup.rs -sSf | sh` for ubuntu 22.04 LTS).


# Execution

1. Run the command to build the project 


# Parameters

Database parameters

**dbPath:** Path to the gtfs database
   
Schedule time window

start_time = gtfs::parse_duration("08:00:00").unwrap();
end_time = gtfs::parse_duration("08:03:00").unwrap();
day = "monday";
speedUp = 10;
batchIntervalSizeInSeconds = 1;
changeFrequencyInSeconds = 1;
    
output paths

topology_path = "fixed_topology.json";
topology_updates_path = "topology_updates.json";
geo_json_path = "geo.json";
    
cell id params

file_path  = "262.csv"
vodafone_mncs = vec![2, 4, 9];
beginning_of_2024 = 1704067200;
min_samples = 10;
radio = "LTE";



## Acknowledgement
The OpenCelliD database file in the project is downloaded from https://opencellid.org under Creative Commons License. OpenCelliD Project is licensed under a Creative Commons Attribution-ShareAlike 4.0 International License
