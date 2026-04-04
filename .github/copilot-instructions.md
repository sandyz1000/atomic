The Atomic project is a rewrite/refactor of the Vega project that runs on stable Rust.

The original Vega project has several issues and does not work on stable Rust. It also uses Cap’n Proto for serialization and deserialization. It uses trait-object serialization/deserialization as well, which I don’t think is necessary.

You can find more details about the Atomic project in the README.md file in the root directory. (DO NOT MODIFY THE README FILE IN THE ROOT DIRECTORY)

The project uses rkyv for serialization and deserialization.

When a user asks about serialization and deserialization in Rust, refer to rkyv before implementing anything.
