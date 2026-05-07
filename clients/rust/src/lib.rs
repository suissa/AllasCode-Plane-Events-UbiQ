use async_nats::{HeaderMap, HeaderValue, Message};
use bytes::Bytes;
use futures::StreamExt;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

pub const LINEAR_EVENT_HEADER: &str = "Nats-Event-Type";
pub const LINEAR_EVENT_TYPE: &str = "Linear";
pub const LINEAR_TTL_HEADER: &str = "Nats-Linear-TTL";
pub const LINEAR_OUTBOX_ID_HEADER: &str = "Nats-Linear-Outbox-Id";
pub const LINEAR_DLQ_REASON_HEADER: &str = "Nats-Linear-DLQ-Reason";
pub const LINEAR_DLQ_ORIGINAL_SUBJECT_HEADER: &str = "Nats-Linear-Original-Subject";
pub const LINEAR_PQC_ALGORITHM_HEADER: &str = "Nats-Linear-PQC-Alg";
pub const LINEAR_PQC_PUBLIC_KEY_HEADER: &str = "Nats-Linear-PQC-Public-Key";
pub const DPOP_HEADER: &str = "DPoP";
pub const LINEAR_PQC_ALGORITHM: &str = "ML-KEM-768";

#[derive(Clone, Debug)]
pub struct OutboxEntry {
    pub id: String,
    pub subject: String,
    pub payload: Bytes,
    pub ttl: Option<Duration>,
    pub attempts: usize,
}

#[derive(Clone, Debug)]
pub struct SecurityOptions {
    pub dpop_token: Option<String>,
    pub pqc_public_key: Option<String>,
}

pub struct OutboxOptions {
    pub max_attempts: usize,
    pub dlq_subject: Option<String>,
    pub security: Option<SecurityOptions>,
}

pub struct Outbox {
    entries: VecDeque<OutboxEntry>,
    next_id: u64,
    max_attempts: usize,
    dlq_subject: Option<String>,
    security: Option<SecurityOptions>,
}

impl Outbox {
    pub fn new(options: OutboxOptions) -> Self {
        Self {
            entries: VecDeque::new(),
            next_id: 0,
            max_attempts: options.max_attempts.max(1),
            dlq_subject: options.dlq_subject,
            security: options.security,
        }
    }

    pub fn enqueue_linear(
        &mut self,
        subject: impl Into<String>,
        payload: impl Into<Bytes>,
        ttl: Option<Duration>,
    ) -> String {
        self.next_id += 1;
        let id = self.next_id.to_string();
        self.entries.push_back(OutboxEntry {
            id: id.clone(),
            subject: subject.into(),
            payload: payload.into(),
            ttl,
            attempts: 0,
        });
        id
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub async fn flush(
        &mut self,
        client: &async_nats::Client,
    ) -> Result<(), async_nats::PublishError> {
        let mut remaining = VecDeque::new();
        while let Some(mut entry) = self.entries.pop_front() {
            match client
                .publish_with_headers(
                    entry.subject.clone(),
                    entry.linear_headers(self.security.as_ref()),
                    entry.payload.clone(),
                )
                .await
            {
                Ok(()) => continue,
                Err(err) => {
                    entry.attempts += 1;
                    if entry.attempts >= self.max_attempts {
                        if let Some(dlq_subject) = &self.dlq_subject {
                            match client
                                .publish_with_headers(
                                    dlq_subject.clone(),
                                    entry.dlq_headers(err.to_string()),
                                    entry.payload.clone(),
                                )
                                .await
                            {
                                Ok(()) => continue,
                                Err(dlq_err) => {
                                    remaining.push_back(entry);
                                    remaining.extend(self.entries.drain(..));
                                    self.entries = remaining;
                                    return Err(dlq_err);
                                }
                            }
                        }
                    }
                    remaining.push_back(entry);
                    remaining.extend(self.entries.drain(..));
                    self.entries = remaining;
                    return Err(err);
                }
            }
        }
        self.entries = remaining;
        Ok(())
    }
}

impl OutboxEntry {
    fn linear_headers(&self, security: Option<&SecurityOptions>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(LINEAR_EVENT_HEADER, HeaderValue::from(LINEAR_EVENT_TYPE));
        headers.insert(LINEAR_OUTBOX_ID_HEADER, HeaderValue::from(self.id.clone()));
        if let Some(ttl) = self.ttl {
            headers.insert(
                LINEAR_TTL_HEADER,
                HeaderValue::from(ttl.as_millis().to_string()),
            );
        }
        apply_security_headers(&mut headers, security);
        headers
    }

    fn dlq_headers(&self, reason: String) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(LINEAR_OUTBOX_ID_HEADER, HeaderValue::from(self.id.clone()));
        headers.insert(LINEAR_DLQ_REASON_HEADER, HeaderValue::from(reason));
        headers.insert(
            LINEAR_DLQ_ORIGINAL_SUBJECT_HEADER,
            HeaderValue::from(self.subject.clone()),
        );
        headers
    }
}

fn apply_security_headers(headers: &mut HeaderMap, security: Option<&SecurityOptions>) {
    if let Some(security) = security {
        if let Some(public_key) = &security.pqc_public_key {
            headers.insert(
                LINEAR_PQC_ALGORITHM_HEADER,
                HeaderValue::from(LINEAR_PQC_ALGORITHM),
            );
            headers.insert(
                LINEAR_PQC_PUBLIC_KEY_HEADER,
                HeaderValue::from(public_key.clone()),
            );
        }
        if let Some(token) = &security.dpop_token {
            headers.insert(DPOP_HEADER, HeaderValue::from(token.clone()));
        }
    }
}

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
    publish_with_security(client, subject, payload, ttl, None).await
}

pub async fn publish_with_security(
    client: &async_nats::Client,
    subject: impl Into<String>,
    payload: impl Into<Bytes>,
    ttl: Option<Duration>,
    security: Option<&SecurityOptions>,
) -> Result<(), async_nats::PublishError> {
    let mut headers = HeaderMap::new();
    headers.insert(LINEAR_EVENT_HEADER, HeaderValue::from(LINEAR_EVENT_TYPE));
    if let Some(ttl) = ttl {
        headers.insert(
            LINEAR_TTL_HEADER,
            HeaderValue::from(ttl.as_millis().to_string()),
        );
    }
    apply_security_headers(&mut headers, security);
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

    #[test]
    fn outbox_enqueue_builds_linear_headers() {
        let mut outbox = Outbox::new(OutboxOptions {
            max_attempts: 3,
            dlq_subject: Some("linear.dlq".to_string()),
            security: None,
        });
        let id = outbox.enqueue_linear(
            "linear.out",
            Bytes::from_static(b"payload"),
            Some(Duration::from_millis(25)),
        );

        assert_eq!(id, "1");
        assert_eq!(outbox.len(), 1);
        let entry = outbox.entries.front().expect("entry");
        let headers = entry.linear_headers(None);
        assert_eq!(
            headers.get(LINEAR_EVENT_HEADER).unwrap().as_str(),
            LINEAR_EVENT_TYPE
        );
        assert_eq!(headers.get(LINEAR_OUTBOX_ID_HEADER).unwrap().as_str(), "1");
        assert_eq!(headers.get(LINEAR_TTL_HEADER).unwrap().as_str(), "25");
    }

    #[test]
    fn security_options_add_pqc_and_dpop_headers() {
        let entry = OutboxEntry {
            id: "secure".to_string(),
            subject: "linear.secure".to_string(),
            payload: Bytes::from_static(b"payload"),
            ttl: None,
            attempts: 0,
        };
        let security = SecurityOptions {
            dpop_token: Some("proof.jwt".to_string()),
            pqc_public_key: Some("kyber-public-key".to_string()),
        };

        let headers = entry.linear_headers(Some(&security));
        assert_eq!(
            headers.get(LINEAR_PQC_ALGORITHM_HEADER).unwrap().as_str(),
            LINEAR_PQC_ALGORITHM
        );
        assert_eq!(
            headers.get(LINEAR_PQC_PUBLIC_KEY_HEADER).unwrap().as_str(),
            "kyber-public-key"
        );
        assert_eq!(headers.get(DPOP_HEADER).unwrap().as_str(), "proof.jwt");
    }

    #[test]
    fn outbox_entry_builds_dlq_headers() {
        let entry = OutboxEntry {
            id: "abc".to_string(),
            subject: "linear.out".to_string(),
            payload: Bytes::from_static(b"payload"),
            ttl: None,
            attempts: 1,
        };

        let headers = entry.dlq_headers("publish failed".to_string());
        assert_eq!(
            headers.get(LINEAR_OUTBOX_ID_HEADER).unwrap().as_str(),
            "abc"
        );
        assert_eq!(
            headers.get(LINEAR_DLQ_REASON_HEADER).unwrap().as_str(),
            "publish failed"
        );
        assert_eq!(
            headers
                .get(LINEAR_DLQ_ORIGINAL_SUBJECT_HEADER)
                .unwrap()
                .as_str(),
            "linear.out"
        );
    }
}
