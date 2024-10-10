use serde::{Serialize, Deserialize};
use bincode::{serialize, deserialize};

#[derive(Serialize, Deserialize)]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

fn main() {
    let serialized = serialize(&add).unwrap();
    let deserialized: fn(i32, i32) -> i32 = deserialize(&serialized).unwrap();

    let result = deserialized(2, 3);
    println!("Result: {}", result);
}