use tracing_appender::non_blocking;
use tracing_subscriber::{fmt, EnvFilter};

pub struct SystemLogging;

impl SystemLogging {
    pub fn init_logging() -> non_blocking::WorkerGuard {
        let (non_blocking, guard) = non_blocking(std::io::stdout());
        
        fmt()
            .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
            .with_timer(fmt::time::UtcTime::rfc_3339())
            .with_file(true)
            .with_ansi(true)
            .with_target(false)
            .with_line_number(true)
            .with_writer(non_blocking)
            .compact()
            .init();
        
        guard
    }
}