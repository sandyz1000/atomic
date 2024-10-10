use std::env;
use std::path::PathBuf;
use std::path::Path;
use std::ffi::OsStr;


trait Converter<T, U> {
    fn convert(&self, obj: T) -> U;
}

struct ConverterInstance<T, U> {
    converter_class: Option<String>,
    default_converter: Box<dyn Converter<T, U>>,
}

impl<T, U> ConverterInstance<T, U> {
    fn new(converter_class: Option<String>, default_converter: Box<dyn Converter<T, U>>) -> ConverterInstance<T, U> {
        ConverterInstance {
            converter_class,
            default_converter,
        }
    }

    fn get_instance(&self) -> Box<dyn Converter<T, U>> {
        match self.converter_class {
            Some(ref cc) => {
                match Utils::class_for_name::<dyn Converter<T, U>>(cc) {
                    Ok(c) => {
                        println!("Loaded converter: {}", cc);
                        c
                    },
                    Err(err) => {
                        println!("Failed to load converter: {}", cc);
                        panic!(err);
                    }
                }
            },
            None => {
                self.default_converter.clone()
            }
        }
    }
}

struct ConverterManager {
    converter_map: HashMap<String, Box<dyn ConverterInstance>>,
}

impl ConverterManager {
    fn new() -> ConverterManager {
        ConverterManager {
            converter_map: HashMap::new(),
        }
    }

    fn get_converter<T, U>(&self, converter_name: &str) -> Option<Box<dyn Converter<T, U>>> {
        match self.converter_map.get(converter_name) {
            Some(ref converter_instance) => {
                Some(converter_instance.get_instance())
            },
            None => {
                None
            }
        }
    }
}


fn python_path() -> String {
    let mut python_path: Vec<String> = Vec::new();

    for home in env::var("ATOMIC_HOME") {
        let home = Path::new(&home);
        // TODO: Fix here
        python_path.push(home.join("python").join("lib").join("pya.zip").to_str().unwrap().to_string());
        python_path.push(home.join("python").join("lib").join("py4j-0.10.7-src.zip").to_str().unwrap().to_string());
    }

    python_path.push(env::current_dir().unwrap().join("target").join("scala-2.11").join("examples_2.11-0.1.0-SNAPSHOT.jar").to_str().unwrap().to_string());
    python_path.push(env::current_dir().unwrap().join("target").join("scala-2.11").join("examples_2.11-0.1.0-SNAPSHOT-tests.jar").to_str().unwrap().to_string());
    python_path.push(env::current_dir().unwrap().join("target").join("scala-2.11").join("examples_2.11-0.1.0-SNAPSHOT-sources.jar").to_str().unwrap().to_string());


    python_path.join(":")
}