pub fn generate_color_palette(num_colors: usize) -> Vec<String> {
    let mut colors = Vec::new();
    for i in 0..num_colors {
        let hue = (i as f64 / num_colors as f64) * 360.0;
        let color = hsl_to_hex(hue, 0.5, 0.5);
        colors.push(color);
    }
    colors
}

fn hsl_to_hex(hue: f64, saturation: f64, lightness: f64) -> String {
    let c = (1.0 - (2.0 * lightness - 1.0).abs()) * saturation;
    let x = c * (1.0 - ((hue / 60.0) % 2.0 - 1.0).abs());
    let m = lightness - c / 2.0;

    let (r, g, b) = match hue as u32 {
        0..=59 => (c, x, 0.0),
        60..=119 => (x, c, 0.0),
        120..=179 => (0.0, c, x),
        180..=239 => (0.0, x, c),
        240..=299 => (x, 0.0, c),
        300..=359 => (c, 0.0, x),
        _ => (0.0, 0.0, 0.0),
    };

    format!(
        "#{:02X}{:02X}{:02X}",
        ((r + m) * 255.0).round() as u8,
        ((g + m) * 255.0).round() as u8,
        ((b + m) * 255.0).round() as u8
    )
}