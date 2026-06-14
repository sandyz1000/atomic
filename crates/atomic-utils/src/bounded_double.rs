use std::convert::From;
use std::fmt::{Display, Error as FmtError, Formatter};

#[derive(Clone, Debug, Copy, PartialEq)]
pub struct BoundedDouble {
    pub mean: f64,
    pub confidence: f64,
    pub low: f64,
    pub high: f64,
}

impl From<(f64, f64, f64, f64)> for BoundedDouble {
    fn from(src: (f64, f64, f64, f64)) -> BoundedDouble {
        let (mean, confidence, low, high) = src;
        BoundedDouble {
            mean,
            confidence,
            low,
            high,
        }
    }
}

impl Display for BoundedDouble {
    fn fmt(&self, fmt: &mut Formatter) -> std::result::Result<(), FmtError> {
        write!(fmt, "[{:.3}, {:.3}]", self.low, self.high)
    }
}

#[cfg(test)]
mod tests {
    use super::BoundedDouble;

    #[test]
    fn display_formats_low_high_interval() {
        let bd = BoundedDouble::from((10.0, 0.95, 8.5, 11.25));
        assert_eq!(format!("{bd}"), "[8.500, 11.250]");
    }
}
