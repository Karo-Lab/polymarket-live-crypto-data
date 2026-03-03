use questdb::ingress::{Buffer, Sender, TimestampNanos};

use exchanges_common::{
    error::IngestionError,
    traits::{ColumnValue, IngestionBackend},
};

#[derive(Debug)]
pub struct QuestDBClient {
    sender: Sender,
    buffer: Buffer,
}

impl QuestDBClient {
    pub fn new(db_url: String) -> Self {
        let sender = Sender::from_conf(db_url).expect("Unable to instantinate quest db client");
        Self {
            buffer: sender.new_buffer(),
            sender,
        }
    }
}

impl IngestionBackend for QuestDBClient {
    async fn push(
        &mut self,
        table: &str,
        columns: Vec<(&str, exchanges_common::traits::ColumnValue<'_>)>,
    ) -> Result<(), IngestionError> {
        let _ = self.buffer.table(table);
        for (col_name, col_value) in columns {
            match col_value {
                ColumnValue::Integer(v) => {
                    self.buffer
                        .column_i64(col_name, v)
                        .map_err(|e| IngestionError::TypeError(e.to_string()))?;
                }
                ColumnValue::Double(v) => {
                    self.buffer
                        .column_f64(col_name, v)
                        .map_err(|e| IngestionError::TypeError(e.to_string()))?;
                }
                ColumnValue::Varchar(v) => {
                    self.buffer
                        .column_str(col_name, v)
                        .map_err(|e| IngestionError::TypeError(e.to_string()))?;
                }
                ColumnValue::Timestamp(ts) => {
                    if col_name == "timestamp" {
                        self.buffer
                            .at(TimestampNanos::new(ts))
                            .map_err(|e| IngestionError::TypeError(e.to_string()))?;
                    } else {
                        self.buffer
                            .column_ts(col_name, TimestampNanos::new(ts))
                            .map_err(|e| IngestionError::TypeError(e.to_string()))?;
                    }
                }
                ColumnValue::Array2dDouble(arr) => {
                    self.buffer
                        .column_arr(col_name, &arr)
                        .map_err(|e| IngestionError::TypeError(e.to_string()))?;
                }
                ColumnValue::Symbol(s) => {
                    self.buffer
                        .symbol(col_name, s)
                        .map_err(|e| IngestionError::TypeError(e.to_string()))?;
                }
                _ => {}
            }
        }
        Ok(())
    }
    async fn flush(&mut self) -> Result<(), IngestionError> {
        self.sender
            .flush(&mut self.buffer)
            .map_err(|e| IngestionError::IngestionFailed(e.to_string()))?;
        Ok(())
    }
}
