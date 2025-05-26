use log::{debug, info, warn, error};

pub struct LogController;

impl LogController {
    pub fn debug(msg: &str) {
        debug!("{}", msg);
    }
    
    pub fn info(msg: &str) {
        info!("{}", msg);
    }
    
    pub fn warn(msg: &str) {
        warn!("{}", msg);
    }
    
    pub fn error(msg: &str) {
        error!("{}", msg);
    }
}