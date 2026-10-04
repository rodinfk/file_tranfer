pub struct Event {
    pub handle: u64,
    pub readable: bool,
    pub error: bool,
}

#[cfg(unix)]
pub fn handle_of<T: std::os::fd::AsRawFd>(s: &T) -> u64 {
    s.as_raw_fd() as u64
}

#[cfg(windows)]
pub fn handle_of<T: std::os::windows::io::AsRawSocket>(s: &T) -> u64 {
    s.as_raw_socket() as u64
}

mod sys {
    use super::Event;
    use std::io;

    #[repr(C)]
    struct WsaPollFd {
        fd: usize,
        events: i16,
        revents: i16,
    }

    const POLLERR: i16 = 0x0001;
    const POLLHUP: i16 = 0x0002;
    const POLLNVAL: i16 = 0x0004;
    const POLLRDNORM: i16 = 0x0100;

    #[link(name = "ws2_32")]
    unsafe extern "system" {
        fn WSAPoll(fds: *mut WsaPollFd, nfds: u32, timeout: i32) -> i32;
    }

    pub struct Poller {
        fds: Vec<WsaPollFd>,
    }

    impl Poller {
        pub fn new() -> io::Result<Self> {
            Ok(Poller { fds: Vec::new() })
        }

        pub fn add(&mut self, h: u64) {
            self.fds.push(WsaPollFd {
                fd: h as usize,
                events: POLLRDNORM,
                revents: 0,
            })
        }

        pub fn del(&mut self, h: u64) {
            if let Some(i) = self.fds.iter().position(|f| f.fd == h as usize) {
                self.fds.swap_remove(i);
            }
        }

        pub fn wait(&mut self, out: &mut Vec<Event>) -> io::Result<()> {
            out.clear();
            let n = unsafe { WSAPoll(self.fds.as_mut_ptr(), self.fds.len() as u32, -1) };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            for f in &self.fds {
                if f.revents != 0 {
                    out.push(Event {
                        handle: f.fd as u64,
                        readable: f.revents & (POLLRDNORM | POLLHUP) != 0,
                        error: f.revents & (POLLERR | POLLNVAL) != 0,
                    });
                }
            }

            Ok(())
        }
    }
}

pub use sys::Poller;
