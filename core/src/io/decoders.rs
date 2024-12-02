use crate::ser_data::SerFunc;
pub struct Decoders {}

impl Decoders {
    pub fn to_utf8<F: SerFunc<Vec<u8>, Output = String>>() -> F {
        let f = serde_closure::Fn!(|file: Vec<u8>| {
            String::from_utf8(file)
                .unwrap()
        });
        f
    }

    pub fn to_utf8_lines<F: SerFunc<Vec<u8>, Output = String>>() -> F {
        let f = serde_closure::Fn!(|file: Vec<u8>| {
            String::from_utf8(file)
                .unwrap()
                .lines()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        });
        f
    }
}
