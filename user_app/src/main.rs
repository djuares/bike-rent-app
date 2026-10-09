#![forbid(unsafe_code)]
#![allow(missing_docs)]
#![allow(dead_code)]

mod config;
mod user_app_actor;

use actix::Actor;
use common::config::load_config;
use config::UserAppConfig;
use user_app_actor::{spawn_stdin_reader, UserAppActor};

fn main() {
    let config: UserAppConfig = load_config("config.json").expect("No se pudo cargar config.json");

    let system = actix::System::new();
    system.block_on(async {
        let app = UserAppActor::new(config).start();
        spawn_stdin_reader(app);
        println!(
            "[user_app] listo — comandos: query | rent <station> <slot> | \
             return <station> <slot> | signal <on|off> | at <ip:puerto>"
        );
    });

    let _ = system.run();
}
