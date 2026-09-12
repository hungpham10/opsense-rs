use std::io::Error;

pub trait Reload {
    fn reload(&self) -> Result<(), Error>;
    fn keys(&self) -> Vec<&str>;
}
