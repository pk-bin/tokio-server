#![allow(unused)]
//! **`raw_session`** TCP Socket server, client 가 사용하는 session 을 가지고 있으며 쓰기, 읽기, 갱신 등의 기능을 지원한다.
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;

/// Raw Packet 의 header size.
const RAW_PACKET_HEADER_SIZE: usize = 52;
/// Raw Packet 의 tail size.
const RAW_PACKET_TAIL_SIZE: usize = 4;

/// maximum chuck size
pub(crate) const MAX_CHUCK_SIZE: usize = 250_000;
/// mapper 의 get_unary_list 를 가리킴.
pub(crate) const GET_UNARY_LIST_DELIMITER: u32 = 0x6EAE0101;
/// mapper 의 get_stream_list 를 가리킴.
pub(crate) const GET_STREAM_LIST_DELIMITER: u32 = 0x6EAE0102;
/// mapper 의 open_unary 를 가리킴.
pub(crate) const OPEN_UNARY_DELIMITER: u32 = 0x6EAE0201;
/// mapper 의 open_stream 를 가리킴.
pub(crate) const OPEN_STREAM_DELIMITER: u32 = 0x6EAE0202;
/// Backend Error 발생시 Frontend 까지 전달하기 위한 프로토콜.
pub(crate) const ERROR_DELIMITER: u32 = 0x6EAE0909;
/// Backend Socket Buffer Receive Maximum Size
const MAX_SOCKET_BUFFER_SIZE: usize = 262_144;
/// Backend Socket Receive Data Store Maximum Size
const MAX_STORE_BUFFER_SIZE: usize = 600_000;

/// raw lumen network buffer 이동시에 사용될 packet 구조체
#[derive(Debug, Clone)]
pub(crate) struct RawPacket {
    /// delimiter
    pub(crate) delimiter: u32,
    /// split 위치
    pub(crate) index: i32,
    /// split 크기
    pub(crate) count: u32,
    /// vector 길이.
    pub(crate) length: u32,
    /// open_number.
    pub(crate) packet_number: u32,
    /// process id
    pub(crate) id: [u8; 32],
    /// 데이터
    pub(crate) data: Vec<u8>,
    /// crc32 등 check 값.
    tail: u32,
}
impl RawPacket {
    /// 생성
    pub(crate) fn new(
        data_delimiter: u32,
        data_index: i32,
        data_count: u32,
        data_length: u32,
        data_number: u32,
        data_id: [u8; 32],
        data_buff: Vec<u8>,
    ) -> Self {
        Self {
            delimiter: data_delimiter,
            index: data_index,
            count: data_count,
            length: data_length,
            packet_number: data_number,
            id: data_id,
            data: data_buff,
            tail: !(data_delimiter + data_index as u32 + data_count + data_length + data_number),
        }
    }

    /// 들어온 buffer 를 사용하여 packet 을 만들때 사용.
    fn valid_and_new(
        window_start: usize,
        window_end: usize,
        store_buffer: &[u8; 600_000],
    ) -> Result<RawPacket, std::io::Error> {
        if window_start + RAW_PACKET_HEADER_SIZE > window_end {
            return Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, ""));
        }

        let mut delimiter_buff = [0; 4];
        let mut packet_index = [0; 4];
        let mut packet_count = [0; 4];
        let mut packet_length = [0; 4];
        let mut packet_nubmer = [0; 4];
        let mut data_id = [0; 32];
        delimiter_buff.copy_from_slice(&store_buffer[window_start..window_start + 4]);
        packet_index.copy_from_slice(&store_buffer[window_start + 4..window_start + 8]);
        packet_count.copy_from_slice(&store_buffer[window_start + 8..window_start + 12]);
        packet_length.copy_from_slice(&store_buffer[window_start + 12..window_start + 16]);
        packet_nubmer.copy_from_slice(&store_buffer[window_start + 16..window_start + 20]);
        data_id.copy_from_slice(&store_buffer[window_start + 20..window_start + 52]);

        let data_delimiter = u32::from_le_bytes(delimiter_buff);
        let data_index = u32::from_le_bytes(packet_index);
        let data_count = u32::from_le_bytes(packet_count);
        let data_number = u32::from_le_bytes(packet_nubmer);
        let data_length = u32::from_le_bytes(packet_length) as usize;

        match data_delimiter {
            GET_UNARY_LIST_DELIMITER
            | GET_STREAM_LIST_DELIMITER
            | OPEN_UNARY_DELIMITER
            | OPEN_STREAM_DELIMITER => {
                return if data_index >= data_count {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, ""));
                } else {
                    if window_start + RAW_PACKET_HEADER_SIZE + data_length > window_end {
                        return Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, ""));
                    } else {
                        let mut tail_buff = [0; 4];
                        tail_buff.copy_from_slice(
                            &store_buffer[window_start + RAW_PACKET_HEADER_SIZE + data_length
                                ..window_start
                                    + RAW_PACKET_HEADER_SIZE
                                    + RAW_PACKET_TAIL_SIZE
                                    + data_length],
                        );
                        let data_tail = u32::from_le_bytes(tail_buff);
                        let mut data_buffer = vec![0; data_length];

                        data_buffer.copy_from_slice(
                            &store_buffer[window_start + RAW_PACKET_HEADER_SIZE
                                ..window_start + RAW_PACKET_HEADER_SIZE + data_length],
                        );
                        if data_tail
                            != !(data_delimiter
                                + data_index
                                + data_count
                                + data_length as u32
                                + data_number)
                        {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, ""));
                        }
                        Ok(RawPacket::new(
                            data_delimiter,
                            data_index as i32,
                            data_count,
                            data_length as u32,
                            data_number,
                            data_id,
                            data_buffer,
                        ))
                    }
                }
            }
            _ => {}
        }
        return Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, ""));
    }

    /// packet 을 binary 화 함. ( serialize library 사용하지 않음 )
    fn serialize(&self) -> Vec<u8> {
        let mut result =
            vec![0; RAW_PACKET_HEADER_SIZE + self.length as usize + RAW_PACKET_TAIL_SIZE];
        result[0..4].copy_from_slice(&self.delimiter.to_le_bytes());
        result[4..8].copy_from_slice(&self.index.to_le_bytes());
        result[8..12].copy_from_slice(&self.count.to_le_bytes());
        result[12..16].copy_from_slice(&self.length.to_le_bytes());
        result[16..20].copy_from_slice(&self.packet_number.to_le_bytes());
        result[20..RAW_PACKET_HEADER_SIZE].copy_from_slice(&self.id);
        result[RAW_PACKET_HEADER_SIZE..(self.length as usize + RAW_PACKET_HEADER_SIZE)]
            .copy_from_slice(&self.data);
        result[(RAW_PACKET_HEADER_SIZE + self.length as usize)
            ..(RAW_PACKET_HEADER_SIZE + self.length as usize + RAW_PACKET_TAIL_SIZE)]
            .copy_from_slice(&self.tail.to_le_bytes());
        result
    }

    /// packet 의 [u8;32] 로 되어있는 id 값을 Box<String> 으로 반환해줌.
    pub(crate) fn get_str_id(&self) -> Box<String> {
        let null_index = self.id.iter().position(|&b| b == 0);
        if let Some(index) = null_index {
            Box::new(String::from_utf8_lossy(&self.id[0..index]).to_string())
        } else {
            Box::new(String::from_utf8_lossy(&self.id).to_string())
        }
    }
}

/// mio::net 로 작성된 tcp session struct
pub(crate) struct RawSession {
    /// 연결되어있는지 확인.
    is_connect: bool,
    /// stream. socket stream 이다.
    pub(crate) stream: TcpStream,
    /// network buffer 와 상호작용하는 buffer.
    read_buffer: [u8; MAX_SOCKET_BUFFER_SIZE],
    /// network buffer 와 상호작용하는 buffer 의 내용을 저장하는 buffer.
    store_buffer: [u8; MAX_STORE_BUFFER_SIZE],
    /// slide window 의 마지막 위치.
    window_end: usize,
    /// store_buffer 에 저장된 데이터를 collect 하는 golang 에서 사용한 double buffer
    inner_buffer: InnerBuffer,
    /// 외부에서 thread 등을 통해 전달되는 packet write 부분을 받아내기 위한 Arc Sender Channel
    pub(crate) shared_tx: tokio::sync::mpsc::Sender<RawPacket>,
    /// Sender Channel 와 한쌍을 이루는 Receiver Channel
    rx: tokio::sync::mpsc::Receiver<RawPacket>,
    /// timeout Duration to u64
    timeout_sec: u64,
    /// timeout 용 instance
    elapsed_now: std::time::Instant,
    /// socket addr info
    sock_addr: std::net::SocketAddr,
}

#[allow(unused_qualifications)]
impl RawSession {
    /// 생성 및 timeout start
    pub(crate) fn new(
        stream: TcpStream,
        sock_addr: std::net::SocketAddr,
        timeout: Option<std::time::Duration>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        match timeout {
            None => Self {
                is_connect: true,
                stream,
                sock_addr,
                read_buffer: [0; MAX_SOCKET_BUFFER_SIZE],
                store_buffer: [0; MAX_STORE_BUFFER_SIZE],
                inner_buffer: InnerBuffer::new(),
                window_end: 0,
                shared_tx: tx,
                rx,
                timeout_sec: 0,
                elapsed_now: std::time::Instant::now(),
            },
            Some(obj) => Self {
                is_connect: true,
                stream,
                sock_addr,
                read_buffer: [0; MAX_SOCKET_BUFFER_SIZE],
                store_buffer: [0; MAX_STORE_BUFFER_SIZE],
                inner_buffer: InnerBuffer::new(),
                window_end: 0,
                shared_tx: tx,
                rx,
                timeout_sec: obj.as_secs(),
                elapsed_now: std::time::Instant::now(),
            },
        }
    }

    /// socket stream 에서 데이터 읽기 및 하나로 합쳐진 RawPacket 반환.
    ///
    /// poll ready 를 사용하여 read 가능해질떄까지 block 함.
    pub(crate) fn socket_read(&mut self) -> Result<Box<Vec<RawPacket>>, std::io::Error> {
        if self.is_connect {
            match self.stream.try_read(&mut self.read_buffer) {
                Ok(read_size) => {
                    if read_size < 1 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionRefused,
                            "connection refused",
                        ));
                    }
                    self.elapsed_now = std::time::Instant::now();
                    let mut packet_list = Box::new(vec![]);
                    let mut window_start: usize = 0;
                    self.store_buffer[self.window_end..(self.window_end + read_size)]
                        .copy_from_slice(&self.read_buffer[..read_size]);
                    self.window_end += read_size;
                    loop {
                        match RawPacket::valid_and_new(
                            window_start,
                            self.window_end,
                            &self.store_buffer,
                        ) {
                            Ok(packet) => {
                                window_start += RAW_PACKET_HEADER_SIZE
                                    + RAW_PACKET_TAIL_SIZE
                                    + packet.length as usize;

                                let packet_id = packet.get_str_id();
                                match self.inner_buffer.append(packet) {
                                    Ok(result) => {
                                        packet_list.push(result);
                                        self.inner_buffer.delete(packet_id);
                                    }
                                    Err(e) => {
                                        if e.kind() != std::io::ErrorKind::WouldBlock {
                                            self.inner_buffer.delete(packet_id);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                if e.kind() == std::io::ErrorKind::WouldBlock {
                                    break;
                                } else {
                                    window_start += RAW_PACKET_HEADER_SIZE + RAW_PACKET_TAIL_SIZE;
                                }
                            }
                        }
                    }
                    // ture : Socket Buffer do not find header. store buffer clear.
                    // false : Socket Buffer find header and create RawPacket.
                    if self.window_end - window_start > (MAX_STORE_BUFFER_SIZE / 2) {
                        self.store_buffer.fill(0);
                        self.window_end = 0;
                    } else {
                        // move array
                        if window_start > 0 {
                            self.store_buffer
                                .copy_within(window_start..self.window_end, 0);
                            self.window_end -= window_start;
                        }
                    }
                    if packet_list.len() > 0 {
                        Ok(packet_list)
                    } else {
                        Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, ""))
                    }
                }
                Err(e) => Err(e),
            }
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "connection refused",
            ))
        }
    }
    /// shared_tx의  내용을 읽어들여 실제 stream 에 쓰기.
    pub(crate) async fn socket_write(&mut self) -> Result<(), std::io::Error> {
        match self.stream.writable().await {
            Ok(_) => match self.rx.try_recv() {
                Ok(raw_packet) => self.unsafe_socket_write(raw_packet),
                Err(e) => match e {
                    tokio::sync::mpsc::error::TryRecvError::Empty => {
                        Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, ""))
                    }
                    tokio::sync::mpsc::error::TryRecvError::Disconnected => {
                        println!("tokio::sync::mpsc::error::TryRecvError::Disconnected");
                        Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionRefused,
                            "channel closed",
                        ))
                    }
                },
            },
            Err(e) => Err(e),
        }
    }

    pub(crate) fn unsafe_socket_write(&self, raw_packet: RawPacket) -> Result<(), std::io::Error> {
        match self.stream.try_write(&*raw_packet.serialize()) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// session 에서 사용하던 모든 요소 제거
    pub(crate) fn clear(&mut self) {
        self.is_connect = false;
        self.inner_buffer.clear();
        self.rx.close();
        self.shared_tx.closed();
    }

    ///session 의 연결상태 확인.
    pub(crate) fn is_connected(&self) -> bool {
        self.is_connect
    }

    /// session 이 timoeout 되었는지 확인.
    pub(crate) fn is_timeout(&self) -> bool {
        if self.timeout_sec < 1 {
            false
        } else {
            if self.elapsed_now.elapsed().as_secs() > self.timeout_sec {
                true
            } else {
                false
            }
        }
    }
}

/// Double Buffer 에서 사용.
#[derive(Debug)]
struct InnerBuffer {
    map_data: std::collections::HashMap<Box<String>, RawPacket>,
}
impl InnerBuffer {
    /// 생성
    fn new() -> Self {
        Self {
            map_data: Default::default(),
        }
    }
    /// hashmap 에서 packet 을 찾아내어 이어붙입니다.
    ///
    /// 실패시 error InvalidData Error 반환.
    fn append(&mut self, data: RawPacket) -> Result<RawPacket, std::io::Error> {
        let id = data.get_str_id();
        // 1개 짜리인 경우 그냥 return 시켜주기.
        if data.count == 1 {
            Ok(data)
        } else {
            match self.map_data.get_mut(&*id) {
                // 없는 경우 hashmap 에 추가. 이떄 input 은 index 가 0이 아니면 error 로 가정함.
                None => {
                    if data.index == 0 {
                        self.map_data.insert(id, data);
                        Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, ""))
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "invalid index",
                        ))
                    }
                }
                // 있는 경우
                Some(obj) => {
                    // 순서와 packet_number가 맞는 경우
                    if obj.index + 1 == data.index && obj.packet_number == data.packet_number {
                        // 끝 부분까지 도달한 경우
                        if obj.index as u32 + 2 == data.count {
                            obj.index = data.index;
                            obj.length += data.length;
                            obj.data.extend(data.data);
                            Ok(obj.clone())
                        } else {
                            obj.index = data.index;
                            obj.length += data.length;
                            obj.data.extend(data.data);
                            Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, ""))
                        }
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "invalid index",
                        ))
                    }
                }
            }
        }
    }

    /// hashmap 에서 packet 을 찾아내어 모든 요소 초기화 수행.
    #[allow(unused)]
    fn delete(&mut self, id: Box<String>) {
        match self.map_data.remove(&*id) {
            None => {}
            Some(_) => {}
        }
    }

    /// hashmap 의 모든 요소를 순회하며 제거
    #[allow(unused)]
    fn clear(&mut self) {
        self.map_data.clear();
    }
}
