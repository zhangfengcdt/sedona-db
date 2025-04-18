use adbc_core::{
    error::Result,
    options::{OptionDatabase, OptionValue},
    Driver, Optionable,
};

use crate::database::SedonaDatabase;

#[derive(Default)]
pub struct SedonaDriver {}

impl Driver for SedonaDriver {
    type DatabaseType = SedonaDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(Self::DatabaseType {})
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = Self::DatabaseType {};
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}
