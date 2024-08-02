mod colors;
mod geo_utils;
pub mod gtfs;
pub mod cell_data;
pub mod nes_simulation;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cell_data_reading() {
        cell_data::read_and_print_cell_data_csv("/home/x/gtfs_cleaning/gtfs-sqlite/262.csv").unwrap();
    }
}
