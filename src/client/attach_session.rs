//! An attach connection whose two halves the caller drives itself.
//!
//! [`super::attach::bridge_once`] owns the whole loop: it takes the terminal,
//! pumps stdin to the socket and the socket to stdout, and returns when the
//! user escapes. That is right for `don attach`, where the process *is* the
//! screen.
//!
//! The TUI needs the same connection with the ends left open. It draws the
//! process into a window on a screen it also owns, so the output half must
//! reach a terminal emulator rather than stdout, and the input half has to
//! pass through a key handler that can steal a prefix. This opens the
//! connection, performs the upgrade, and hands back the two halves plus the
//! session id — the caller decides what to do with them.

use super::ClientError;
use std::path::Path;
use tokio::io::{AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::UnixStream;

/// A live attach connection, split for independent reading and writing.
pub struct AttachSession {
    /// Process output. Feed it to an emulator.
    pub reader: ReadHalf<UnixStream>,
    /// Process input. Write raw bytes.
    pub writer: WriteHalf<UnixStream>,
    /// Bytes that arrived alongside the upgrade response, before the split.
    pub leftover: Vec<u8>,
    /// Identifies this session to the resize endpoint.
    pub session_id: Option<u64>,
}

/// Open an attach connection at the given grid size and upgrade it.
///
/// `cols`/`rows` are the size the *process* should believe it has — for the
/// TUI that is its window's inner rectangle, not the terminal.
pub async fn open(
    socket_path: &Path,
    name: &str,
    cols: u16,
    rows: u16,
) -> Result<AttachSession, ClientError> {
    let mut stream = UnixStream::connect(socket_path).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound
            || e.kind() == std::io::ErrorKind::ConnectionRefused
        {
            ClientError::NotRunning {
                path: socket_path.to_path_buf(),
            }
        } else {
            ClientError::Io(e)
        }
    })?;

    let pid = std::process::id();
    let path = format!(
        "/attach/{}?pid={pid}&cols={cols}&rows={rows}",
        super::urlencode(name),
    );
    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Connection: Upgrade\r\n\
         Upgrade: don-attach\r\n\
         \r\n"
    );
    stream.write_all(req.as_bytes()).await?;

    let (status, headers, leftover) = super::read_head(&mut stream).await?;
    if status != 101 {
        let body = super::drain_body(&mut stream, &headers, leftover).await?;
        return Err(super::classify_error(status, &body));
    }
    let session_id = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-don-attach-session"))
        .and_then(|(_, v)| v.trim().parse::<u64>().ok());

    let (reader, writer) = tokio::io::split(stream);
    Ok(AttachSession {
        reader,
        writer,
        leftover,
        session_id,
    })
}

/// Tell the server the process's grid changed size.
///
/// A separate connection, because the attach stream itself carries only
/// process bytes in both directions — there is no room in it for a control
/// message that is not also something the process would read.
pub async fn resize(
    socket_path: &Path,
    name: &str,
    session_id: Option<u64>,
    cols: u16,
    rows: u16,
) -> Result<(), ClientError> {
    super::attach::send_resize_public(socket_path, name, session_id, cols, rows).await
}
