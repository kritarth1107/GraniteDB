// ============================================================================
// GraniteDB — CLI Client
// ============================================================================
// Interactive command-line client for connecting to a GraniteDB server.
// ============================================================================

use clap::Parser;
use serde_json::json;
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;
use uuid::Uuid;

/// GraniteDB CLI Client
#[derive(Parser, Debug)]
#[command(
    name = "granite-cli",
    version = "0.1.0",
    about = "GraniteDB interactive CLI client"
)]
struct Cli {
    /// Server host
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Server port
    #[arg(short, long, default_value = "6380")]
    port: u16,

    /// Database to use
    #[arg(short, long, default_value = "default")]
    database: String,
}

fn main() {
    let cli = Cli::parse();
    let addr = format!("{}:{}", cli.host, cli.port);

    println!("╔══════════════════════════════════════════════════╗");
    println!("║             GraniteDB CLI v0.1.0                ║");
    println!("╚══════════════════════════════════════════════════╝");
    println!("Connecting to {}...", addr);

    let stream = match TcpStream::connect(&addr) {
        Ok(s) => {
            println!("Connected! Type 'help' for commands, 'exit' to quit.");
            println!("Using database: {}", cli.database);
            println!();
            s
        }
        Err(e) => {
            eprintln!("Failed to connect to {}: {}", addr, e);
            eprintln!("Make sure the GraniteDB server is running.");
            std::process::exit(1);
        }
    };

    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut writer = stream;
    let mut current_db = cli.database;

    loop {
        print!("granite:{}> ", current_db);
        io::stdout().flush().unwrap();

        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            break;
        }
        let input = input.trim();
        if input.is_empty() {
            continue;
        }

        match input {
            "exit" | "quit" => {
                println!("Goodbye!");
                break;
            }
            "help" => {
                print_help();
                continue;
            }
            _ => {}
        }

        // Parse command and send
        let request = match parse_cli_command(input, &current_db) {
            Some(req) => req,
            None => {
                if input.starts_with("use ") {
                    current_db = input[4..].trim().to_string();
                    println!("Switched to database: {}", current_db);
                    continue;
                }
                println!("Unknown command. Type 'help' for available commands.");
                continue;
            }
        };

        let request_json = serde_json::to_string(&request).unwrap();
        if writeln!(writer, "{}", request_json).is_err() {
            eprintln!("Connection lost.");
            break;
        }
        writer.flush().unwrap();

        // Read response
        let mut response_line = String::new();
        match reader.read_line(&mut response_line) {
            Ok(0) => {
                eprintln!("Connection closed by server.");
                break;
            }
            Ok(_) => {
                if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&response_line) {
                    println!("{}", serde_json::to_string_pretty(&resp).unwrap());
                } else {
                    println!("{}", response_line.trim());
                }
            }
            Err(e) => {
                eprintln!("Read error: {}", e);
                break;
            }
        }
        println!();
    }
}

fn parse_cli_command(input: &str, db: &str) -> Option<serde_json::Value> {
    let parts: Vec<&str> = input.splitn(2, ' ').collect();
    let cmd = parts[0].to_lowercase();
    let args = parts.get(1).unwrap_or(&"").trim();

    let request_id = Uuid::new_v4().to_string();

    let command = match cmd.as_str() {
        "ping" => json!({ "type": "ping" }),
        "status" => json!({ "type": "server_status" }),
        "dbs" | "databases" => json!({ "type": "list_databases" }),
        "collections" => json!({
            "type": "list_collections",
            "database": db
        }),
        "createdb" => json!({
            "type": "create_database",
            "name": args
        }),
        "createcol" | "createcollection" => json!({
            "type": "create_collection",
            "database": db,
            "name": args
        }),
        "insert" => {
            // insert <collection> <json>
            let parts: Vec<&str> = args.splitn(2, ' ').collect();
            if parts.len() < 2 {
                println!("Usage: insert <collection> <json document>");
                return None;
            }
            let doc: serde_json::Value = serde_json::from_str(parts[1]).ok()?;
            json!({
                "type": "insert_one",
                "database": db,
                "collection": parts[0],
                "document": doc
            })
        }
        "find" => {
            // find <collection> [filter json]
            let parts: Vec<&str> = args.splitn(2, ' ').collect();
            let collection = parts[0];
            let filter = if parts.len() > 1 {
                serde_json::from_str(parts[1]).unwrap_or(json!({}))
            } else {
                json!({})
            };
            json!({
                "type": "find",
                "database": db,
                "collection": collection,
                "filter": filter
            })
        }
        "count" => {
            let parts: Vec<&str> = args.splitn(2, ' ').collect();
            let collection = parts[0];
            let filter = if parts.len() > 1 {
                serde_json::from_str(parts[1]).unwrap_or(json!({}))
            } else {
                json!({})
            };
            json!({
                "type": "count",
                "database": db,
                "collection": collection,
                "filter": filter
            })
        }
        "delete" => {
            let parts: Vec<&str> = args.splitn(2, ' ').collect();
            if parts.len() < 2 {
                println!("Usage: delete <collection> <filter json>");
                return None;
            }
            let filter: serde_json::Value = serde_json::from_str(parts[1]).ok()?;
            json!({
                "type": "delete_many",
                "database": db,
                "collection": parts[0],
                "filter": filter
            })
        }
        _ => return None,
    };

    Some(json!({
        "request_id": request_id,
        "command": command
    }))
}

fn print_help() {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                    GraniteDB CLI Commands                   ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  General:                                                   ║");
    println!("║    ping             — Ping the server                       ║");
    println!("║    status           — Show server status                    ║");
    println!("║    help             — Show this help                        ║");
    println!("║    exit / quit      — Exit the CLI                          ║");
    println!("║                                                             ║");
    println!("║  Database:                                                  ║");
    println!("║    use <db>         — Switch database                       ║");
    println!("║    dbs              — List all databases                    ║");
    println!("║    createdb <name>  — Create a database                     ║");
    println!("║                                                             ║");
    println!("║  Collection:                                                ║");
    println!("║    collections          — List collections                  ║");
    println!("║    createcol <name>     — Create a collection               ║");
    println!("║                                                             ║");
    println!("║  CRUD:                                                      ║");
    println!("║    insert <col> <json>  — Insert a document                 ║");
    println!("║    find <col> [filter]  — Find documents                    ║");
    println!("║    count <col> [filter] — Count documents                   ║");
    println!("║    delete <col> <filter>— Delete documents                  ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
}
