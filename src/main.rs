mod network_tcp;

#[tokio::main]
async fn main() {
    println!("server start!!!");
    network_tcp::server::RawService::start("127.0.0.1:7777".parse().unwrap(), None).await;
}
