use crate::AppError;

use super::{APP, info};
use sqlx::migrate::Migrator;
use std::{convert::TryInto, env};

enum Action {
    Up,
    Down,
}

impl TryInto<Action> for String {
    type Error = String;

    fn try_into(self) -> Result<Action, Self::Error> {
        match &self[..] {
            "up" => Ok(Action::Up),
            "down" => Ok(Action::Down),
            _ => Err("cannot parse command line arg".into()),
        }
    }
}

pub async fn run_migrations() -> Result<(), AppError> {
    let app = &*APP;

    let action: Action = env::args()
        .nth(1)
        .expect("invalid usage use up/down as first argument to run migrations")
        .try_into()
        .expect("invalid usage use up/down as first argument to run migrations");

    let mut migrations_path = app
        .config
        .real_path
        .as_ref()
        .expect("invalid config path")
        .clone();

    migrations_path.extend(["migrations"].iter());

    let migrator = Migrator::new(migrations_path).await?;
    let pools = app.postgres.connect().await?;
    let master = pools.master().await?;

    match action {
        Action::Up => {
            info!("Running migrations");
            migrator.run(master).await?;
        }
        Action::Down => {
            info!("Revert last migration");
            migrator.undo(master, 1).await?;
        }
    };

    Ok(())
}
