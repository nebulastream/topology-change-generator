use geo::VincentyDistance;

fn point_from_lat_lon(lat: f64, lon: f64) -> geo::Point<f64> {
    geo::Point::new(lon, lat)
}

pub(crate) fn vincenty_dist_between_coordinates((lat1, lon1): (f64, f64), (lat2, lon2): (f64, f64)) -> f64 {
    let p1 = point_from_lat_lon(lat1, lon1);
    let p2 = point_from_lat_lon(lat2, lon2);
    p1.vincenty_distance(&p2).unwrap()
}