use async_nats::{HeaderMap, HeaderValue, Message};
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

pub const LINEAR_EVENT_HEADER: &str = "Nats-Event-Type";
pub const LINEAR_EVENT_TYPE: &str = "Linear";
pub const LINEAR_TTL_HEADER: &str = "Nats-Linear-TTL";

pub struct LinearMessage {
    pub subject: String,
    pub reply: Option<String>,
    pub headers: Option<HeaderMap>,
    payload: Arc<Mutex<Option<Bytes>>>,
    linear: bool,
}

impl LinearMessage {
    pub async fn access(&self) -> Option<Bytes> {
        let mut payload = self.payload.lock().await;
        if self.linear {
            payload.take()
        } else {
            payload.clone()
        }
    }

    pub async fn destroy(&self) {
        self.payload.lock().await.take();
    }
}

pub async fn publish(
    client: &async_nats::Client,
    subject: impl Into<String>,
    payload: impl Into<Bytes>,
    ttl: Option<Duration>,
) -> Result<(), async_nats::PublishError> {
    let mut headers = HeaderMap::new();
    headers.insert(LINEAR_EVENT_HEADER, HeaderValue::from(LINEAR_EVENT_TYPE));
    if let Some(ttl) = ttl {
        headers.insert(
            LINEAR_TTL_HEADER,
            HeaderValue::from(ttl.as_millis().to_string()),
        );
    }
    client
        .publish_with_headers(subject.into(), headers, payload.into())
        .await
}

pub async fn subscribe<F, Fut>(
    client: &async_nats::Client,
    subject: impl Into<String>,
    mut handler: F,
) -> Result<(), async_nats::SubscribeError>
where
    F: FnMut(LinearMessage) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut sub = client.subscribe(subject.into()).await?;
    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            handler(from_message(msg)).await;
        }
    });
    Ok(())
}

fn from_message(msg: Message) -> LinearMessage {
    let linear = msg
        .headers
        .as_ref()
        .and_then(|h| h.get(LINEAR_EVENT_HEADER))
        .is_some_and(|v| v.as_str() == LINEAR_EVENT_TYPE);
    let ttl = msg
        .headers
        .as_ref()
        .filter(|_| linear)
        .and_then(|h| h.get(LINEAR_TTL_HEADER))
        .and_then(|v| v.as_str().parse::<u64>().ok())
        .map(Duration::from_millis);
    let payload = Arc::new(Mutex::new(Some(msg.payload)));
    if let Some(ttl) = ttl {
        let payload = Arc::clone(&payload);
        tokio::spawn(async move {
            sleep(ttl).await;
            payload.lock().await.take();
        });
    }
    LinearMessage {
        subject: msg.subject.to_string(),
        reply: msg.reply.map(|s| s.to_string()),
        headers: msg.headers,
        payload,
        linear,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_nats::Subject;

    fn message(payload: &'static str, linear: bool, ttl_ms: Option<u64>) -> Message {
        let mut headers = HeaderMap::new();
        if linear {
            headers.insert(LINEAR_EVENT_HEADER, HeaderValue::from(LINEAR_EVENT_TYPE));
        }
        if let Some(ttl_ms) = ttl_ms {
            headers.insert(LINEAR_TTL_HEADER, HeaderValue::from(ttl_ms.to_string()));
        }
        Message {
            subject: Subject::from("linear.test"),
            reply: None,
            payload: Bytes::from_static(payload.as_bytes()),
            headers: Some(headers),
            status: None,
            description: None,
            length: payload.len(),
        }
    }

    #[tokio::test]
    async fn linear_access_destroys_payload() {
        let msg = from_message(message("secret", true, None));

        assert_eq!(msg.access().await.as_deref(), Some(&b"secret"[..]));
        assert!(msg.access().await.is_none());
    }

    #[tokio::test]
    async fn linear_ttl_destroys_unread_payload() {
        let msg = from_message(message("expires", true, Some(10)));

        sleep(Duration::from_millis(50)).await;
        assert!(msg.access().await.is_none());
    }

    #[tokio::test]
    async fn non_linear_access_is_reusable_and_ignores_ttl() {
        let msg = from_message(message("reusable", false, Some(10)));

        sleep(Duration::from_millis(50)).await;
        assert_eq!(msg.access().await.as_deref(), Some(&b"reusable"[..]));
        assert_eq!(msg.access().await.as_deref(), Some(&b"reusable"[..]));
    }
}
