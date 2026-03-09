use bytes::Bytes;
use futures_util::{Sink, Stream, StreamExt};
use impcurl::ImpersonateTarget;
use impcurl_ws::{CloseCode, CloseFrame, Message, ControlFrameMode, WsConnection};
use std::time::Duration;

fn assert_stream_sink<T>()
where
    T: Stream<Item = Result<Message, impcurl_ws::Error>> + Sink<Message, Error = impcurl_ws::Error>,
{
}

#[test]
fn ws_connection_exposes_stream_and_sink_traits() {
    assert_stream_sink::<WsConnection>();

    let _split = |conn: WsConnection| conn.split();
}

#[test]
fn message_variants_cover_full_websocket_surface() {
    let text = Message::Text("hello".to_owned());
    let binary = Message::Binary(Bytes::from_static(b"hello"));
    let ping = Message::Ping(Bytes::from_static(b"ping"));
    let pong = Message::Pong(Bytes::from_static(b"pong"));
    let close = Message::Close(Some(CloseFrame {
        code: CloseCode::NORMAL,
        reason: "bye".to_owned(),
    }));

    assert!(matches!(text, Message::Text(_)));
    assert!(matches!(binary, Message::Binary(_)));
    assert!(matches!(ping, Message::Ping(_)));
    assert!(matches!(pong, Message::Pong(_)));
    assert!(matches!(close, Message::Close(Some(_))));
}

#[test]
fn builder_keeps_public_connection_configuration() {
    let builder = WsConnection::builder("wss://example.com/ws")
        .header("Origin", "https://example.com")
        .proxy("socks5h://127.0.0.1:1080")
        .impersonate(ImpersonateTarget::Chrome136)
        .connect_timeout(Duration::from_secs(10))
        .verbose(true)
        .control_frame_mode(ControlFrameMode::Manual)
        .read_buffer_messages(16)
        .write_buffer_messages(16);

    let _ = builder;
}
