#![allow(unused)]
//! **`raw_server`** TCP Socket 의 server 영역을 작성하였으며 session table 등을 관리 및 데이터 송/수신등을 구현한다.
use crate::network_tcp::session::{
    RawPacket, RawSession, GET_STREAM_LIST_DELIMITER, GET_UNARY_LIST_DELIMITER, MAX_CHUCK_SIZE,
    OPEN_STREAM_DELIMITER, OPEN_UNARY_DELIMITER,
};
use std::io::Error;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;

#[async_trait::async_trait]
pub(crate) trait RawServerTrait: Send + Sync + 'static {
    /// mapper 에 등록된 unary list 를 가져옴.
    async fn get_unary_list(&self) -> Result<Vec<String>, std::io::Error>;
    /// mapper 에 등록된 stream list 를 가져옴.
    async fn get_stream_list(&self) -> Result<Vec<String>, std::io::Error>;
    /// id 값에 매칭되는 mapper::unary 정보를 넣고 가져옴.
    async fn open_unary(
        &self,
        id: Box<String>,
        param_binary: Vec<u8>,
    ) -> Result<Vec<u8>, std::io::Error>;
    async fn open_stream(
        &self,
        id: Box<String>,
        param_binary: Vec<u8>,
    ) -> Result<(), std::io::Error>;
}

pub(crate) struct RawService {}
impl RawService {
    fn new() -> Self {
        Self {}
    }

    pub(crate) async fn start(
        sock_addr: std::net::SocketAddr,
        session_timeout: Option<std::time::Duration>,
    ) {
        RawServer::new(Self::new())
            .start(sock_addr, session_timeout)
            .await;
    }
}

#[async_trait::async_trait]
impl RawServerTrait for RawService {
    async fn get_unary_list(&self) -> Result<Vec<String>, std::io::Error> {
        let mut test = vec![];
        test.push(String::from("echo1"));
        Ok(test)
    }

    async fn get_stream_list(&self) -> Result<Vec<String>, std::io::Error> {
        let mut test = vec![];
        test.push(String::from("echo2"));
        Ok(test)
    }

    async fn open_unary(
        &self,
        _id: Box<String>,
        _param_binary: Vec<u8>,
    ) -> Result<Vec<u8>, std::io::Error> {
        Ok(vec![0; 10_000])
    }

    async fn open_stream(
        &self,
        id: Box<String>,
        param_binary: Vec<u8>,
    ) -> Result<(), std::io::Error> {
        Ok(())
    }
}

/// mio TcpListener 를 사용하여 작성한 동기형 network server.
///
/// main thread 를 사용하여 동작.
struct RawServer<T: RawServerTrait> {
    /// lumen_Raw_Service
    _inner: Arc<T>,
    session_table:
        Arc<tokio::sync::Mutex<std::collections::HashMap<std::net::SocketAddr, RawSession>>>,
}

impl<T: RawServerTrait> RawServer<T> {
    /// 생성
    fn new(inner: T) -> Self {
        Self {
            _inner: Arc::new(inner),
            session_table: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::default())),
        }
    }

    /// server 동작.
    async fn start(
        &mut self,
        sock_addr: std::net::SocketAddr,
        _session_timeout: Option<std::time::Duration>,
    ) {
        if let Ok(listener) = tokio::net::TcpListener::bind(sock_addr).await {
            let arc_read_session_table = self.session_table.clone();
            let arc_write_session_table = self.session_table.clone();
            let arc_inner = self._inner.clone();
            //read thread
            tokio::spawn(async move {
                let mut delay_interval =
                    tokio::time::interval(tokio::time::Duration::from_millis(1));
                loop {
                    delay_interval.tick().await;
                    let mut lock_session_table = arc_read_session_table.lock().await;
                    for (address, session) in lock_session_table.iter_mut() {
                        if session.is_connected() {
                            if session.is_timeout() {
                                session.clear();
                                println!("{} disconnect", address.to_string());
                            } else {
                                match session.socket_read() {
                                    Ok(read_packet) => {
                                        for item in read_packet.iter() {
                                            match item.delimiter {
                                                GET_UNARY_LIST_DELIMITER => {
                                                    println!("GET_UNARY_LIST_DELIMITER");
                                                    write_unary_list(session, arc_inner.clone())
                                                        .await;
                                                }
                                                GET_STREAM_LIST_DELIMITER => {
                                                    println!("GET_STREAM_LIST_DELIMITER");
                                                    write_stream_list(session, arc_inner.clone())
                                                        .await;
                                                }
                                                OPEN_UNARY_DELIMITER => {
                                                    write_unary_data(
                                                        session,
                                                        item.clone(),
                                                        arc_inner.clone(),
                                                    )
                                                    .await;
                                                }
                                                OPEN_STREAM_DELIMITER => {
                                                    write_stream_data(
                                                        session,
                                                        item.clone(),
                                                        arc_inner.clone(),
                                                    )
                                                    .await;
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        if e.kind() != std::io::ErrorKind::WouldBlock {
                                            session.clear();
                                            println!("{} disconnect", address.to_string());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            });
            //write thread
            tokio::spawn(async move {
                loop {
                    let mut lock_session_table = arc_write_session_table.lock().await;
                    for (address, session) in lock_session_table.iter_mut() {
                        if session.is_connected() {
                            match session.socket_write().await {
                                Ok(_) => {}
                                Err(e) => {
                                    if e.kind() != std::io::ErrorKind::WouldBlock {
                                        println!("session.socket_write error : {}", e);
                                    }
                                }
                            }
                        }
                    }
                }
            });
            loop {
                if let Ok((socket, address)) = listener.accept().await {
                    println!("client is {} connected", address.to_string());
                    // session timeout remove
                    self.session_table
                        .lock()
                        .await
                        .retain(|_, session| !session.is_connected());
                    // session create
                    self.session_table
                        .lock()
                        .await
                        .entry(address)
                        .or_insert(RawSession::new(socket, address, _session_timeout));
                } else {
                    break;
                }
            }
            println!("closed TcpListener");
        }
    }
}

/// private 함수. lumen network mapper 의 get_unary_list 대치.
async fn write_unary_list<T: RawServerTrait>(session: &mut RawSession, inner: Arc<T>) {
    match inner.get_unary_list().await {
        Ok(key_list) => {
            let empty_id: [u8; 32] = [0; 32];
            let key_binary = key_list.join(",").into_bytes();
            session
                .shared_tx
                .try_send(RawPacket::new(
                    GET_UNARY_LIST_DELIMITER,
                    0,
                    1,
                    key_binary.len() as u32,
                    0,
                    empty_id,
                    key_binary,
                ))
                .expect("");
        }
        Err(_) => {}
    }
}

/// private 함수. lumen network mapper 의 get_stream_list 대치.
async fn write_stream_list<T: RawServerTrait>(session: &mut RawSession, inner: Arc<T>) {
    match inner.get_stream_list().await {
        Ok(key_list) => {
            let empty_id: [u8; 32] = [0; 32];
            let key_binary = key_list.join(",").into_bytes();
            session
                .shared_tx
                .try_send(RawPacket::new(
                    GET_STREAM_LIST_DELIMITER,
                    0,
                    1,
                    key_binary.len() as u32,
                    0,
                    empty_id,
                    key_binary,
                ))
                .expect("");
        }
        Err(_) => {}
    }
}

/// private 함수. lumen network mapper 의 open_unary 대치.
async fn write_unary_data<T: RawServerTrait>(
    session: &mut RawSession,
    read_packet: RawPacket,
    inner: Arc<T>,
) {
    match inner
        .open_unary(read_packet.get_str_id(), read_packet.data)
        .await
    {
        Ok(data) => {
            let data_len = data.len();
            let mut point = 0;
            let count = (data_len / MAX_CHUCK_SIZE) + (((data_len % MAX_CHUCK_SIZE) > 0) as usize);
            for i in 0..count {
                if point + MAX_CHUCK_SIZE > data_len {
                    if let Err(e) = session.shared_tx.try_send(RawPacket::new(
                        read_packet.delimiter,
                        i as i32,
                        count as u32,
                        (data_len - point) as u32,
                        read_packet.packet_number,
                        read_packet.id,
                        data[point..].to_vec(),
                    )) {
                        println!("111 : {}", e);
                    }
                } else {
                    if let Err(e) = session.shared_tx.try_send(RawPacket::new(
                        read_packet.delimiter,
                        i as i32,
                        count as u32,
                        MAX_CHUCK_SIZE as u32,
                        read_packet.packet_number,
                        read_packet.id,
                        data[point..(point + MAX_CHUCK_SIZE)].to_vec(),
                    )) {
                        println!("222 : {}", e);
                    }
                    point += MAX_CHUCK_SIZE;
                }
            }
        }
        Err(_) => {}
    }
}

async fn write_stream_data<T: RawServerTrait>(
    session: &RawSession,
    read_packet: RawPacket,
    inner: Arc<T>,
) {
    let arc_sender = session.shared_tx.clone();
    // testing 용으로 inner 사용하지 않음.
    tokio::spawn(async move {
        let mut delay_interval2 = tokio::time::interval(tokio::time::Duration::from_millis(10));
        let mut delay_interval = tokio::time::interval(tokio::time::Duration::from_millis(1));
        let data = vec![0; 8_000_000];
        let mut closed = false;
        let mut ttss = 0;
        loop {
            delay_interval2.tick().await;
            if closed {
                break;
            }
            println!("ttss : {}", ttss);
            ttss += 1;
            let data_len = data.len();
            let mut point = 0;
            let count = (data_len / MAX_CHUCK_SIZE) + (((data_len % MAX_CHUCK_SIZE) > 0) as usize);
            for i in 0..count {
                delay_interval.tick().await;
                if point + MAX_CHUCK_SIZE > data_len {
                    if let Err(e) = arc_sender
                        .send(RawPacket::new(
                            read_packet.delimiter,
                            i as i32,
                            count as u32,
                            (data_len - point) as u32,
                            read_packet.packet_number,
                            read_packet.id,
                            data[point..].to_vec(),
                        ))
                        .await
                    {
                        closed = true;
                        break;
                    }
                } else {
                    if let Err(e) = arc_sender
                        .send(RawPacket::new(
                            read_packet.delimiter,
                            i as i32,
                            count as u32,
                            MAX_CHUCK_SIZE as u32,
                            read_packet.packet_number,
                            read_packet.id,
                            data[point..(point + MAX_CHUCK_SIZE)].to_vec(),
                        ))
                        .await
                    {
                        closed = true;
                        break;
                    }
                    point += MAX_CHUCK_SIZE;
                }
            }
        }
    });
}
