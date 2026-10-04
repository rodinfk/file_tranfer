use file_transfer::{
    STATUS_OK, parse_header,
    poller::{Event, Poller, handle_of},
};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
};

enum State {
    Header,
    Body {
        file: File,
        tmp: PathBuf,
        dst: PathBuf,
        remaining: u64,
    },
}

struct Conn {
    id: u64,
    sock: TcpStream,
    buf: Vec<u8>,
    state: State,
    name: String,
}

impl Conn {
    fn feed(&mut self, data: &[u8], dir: &Path) -> io::Result<bool> {
        if let State::Header = self.state {
            self.buf.extend_from_slice(data);

            let Some((name, size, used)) = parse_header(&self.buf)? else {
                return Ok(false);
            };

            let safe = Path::new(&name)
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "bad file name"))?
                .to_owned();

            if cfg!(windows) && safe.contains(':') {
                return Err(io::Error::new(ErrorKind::InvalidData, "bad file name"));
            }

            let dst = dir.join(&safe);
            let tmp = dir.join(format!("{safe}.{}.part", self.id));
            let file = File::create(&tmp)?;
            let rest = self.buf.split_off(used);

            self.buf.clear();
            self.name = safe;
            self.state = State::Body {
                file,
                tmp,
                dst,
                remaining: size,
            };

            return self.write_body(&rest);
        }
        self.write_body(data)
    }

    fn write_body(&mut self, data: &[u8]) -> io::Result<bool> {
        let State::Body {
            file,
            tmp,
            dst,
            remaining,
        } = &mut self.state
        else {
            unreachable!()
        };

        let take = (*remaining).min(data.len() as u64) as usize;

        file.write_all(&data[..take])?;
        *remaining -= take as u64;

        if *remaining == 0 {
            file.flush()?;
            let (tmp, dst) = (tmp.clone(), dst.clone());
            self.state = State::Header;
            fs::rename(&tmp, &dst)?;
            self.sock.write_all(&[STATUS_OK])?;
            return Ok(true);
        }
        Ok(false)
    }

    fn cleanup(&mut self) {
        if let State::Body { tmp, .. } = std::mem::replace(&mut self.state, State::Header) {
            let _ = fs::remove_file(tmp);
        }
    }
}

fn main() -> io::Result<()> {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:7878".into());
    let dir = PathBuf::from(args.next().unwrap_or_else(|| "received".into()));
    fs::create_dir_all(&dir)?;

    let listener = TcpListener::bind(&addr)?;
    listener.set_nonblocking(true)?;
    let lfd = handle_of(&listener);

    let mut poller = Poller::new()?;
    poller.add(lfd);
    println!("Слушаю {addr}, сохраняю в {}", dir.display());

    let mut conns: HashMap<u64, Conn> = HashMap::new();
    let mut events: Vec<Event> = Vec::new();
    let mut chunk = vec![0u8; 64 * 1024];
    let mut next_id = 0u64;

    loop {
        poller.wait(&mut events)?;
        for ev in events.drain(..) {
            let h = ev.handle;

            if h == lfd {
                loop {
                    match listener.accept() {
                        Ok((sock, peer)) => {
                            sock.set_nonblocking(true)?;
                            let ch = handle_of(&sock);
                            poller.add(ch);
                            next_id += 1;
                            println!("[{peer}] подключился (id {next_id})");
                            conns.insert(
                                ch,
                                Conn {
                                    id: next_id,
                                    sock,
                                    buf: Vec::new(),
                                    state: State::Header,
                                    name: String::new(),
                                },
                            );
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(e) => {
                            eprintln!("accept: {e}");
                            break;
                        }
                    }
                }
                continue;
            }

            let Some(conn) = conns.get_mut(&h) else {
                continue;
            };
            // Одно чтение за событие: быстрый клиент не вытесняет остальных.
            let result: io::Result<bool> = if ev.error {
                Err(io::Error::new(ErrorKind::Other, "socket error"))
            } else if ev.readable {
                match conn.sock.read(&mut chunk) {
                    Ok(0) => Err(io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "клиент закрыл соединение раньше времени",
                    )),
                    Ok(n) => conn.feed(&chunk[..n], &dir),
                    Err(e)
                        if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) =>
                    {
                        Ok(false)
                    }
                    Err(e) => Err(e),
                }
            } else {
                Ok(false)
            };

            match result {
                Ok(false) => {}
                Ok(true) => {
                    println!("[id {}] получен {}", conn.id, conn.name);
                    poller.del(h);
                    conns.remove(&h);
                }
                Err(e) => {
                    eprintln!("[id {}] ошибка: {e}", conn.id);
                    conn.cleanup();
                    poller.del(h);
                    conns.remove(&h);
                }
            }
        }
    }
}
