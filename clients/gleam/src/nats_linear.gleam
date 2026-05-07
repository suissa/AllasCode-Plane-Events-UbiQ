import gleam/int
import gleam/list

pub const linear_event_header = "Nats-Event-Type"
pub const linear_event_type = "Linear"
pub const linear_ttl_header = "Nats-Linear-TTL"
pub const linear_outbox_id_header = "Nats-Linear-Outbox-Id"
pub const linear_dlq_reason_header = "Nats-Linear-DLQ-Reason"
pub const linear_dlq_original_subject_header = "Nats-Linear-Original-Subject"
pub const linear_pqc_algorithm_header = "Nats-Linear-PQC-Alg"
pub const linear_pqc_public_key_header = "Nats-Linear-PQC-Public-Key"
pub const dpop_header = "DPoP"
pub const linear_pqc_algorithm = "ML-KEM-768"

pub type Header =
  #(String, String)

pub type LinearMessage {
  LinearMessage(
    subject: String,
    reply: Option(String),
    headers: List(Header),
    payload: String,
    is_linear: Bool,
    is_destroyed: Bool,
  )
}

pub type SecurityEvidence {
  SecurityEvidence(pqc_public_key: Option(String), dpop_proof: Option(String))
}

pub type OutboxConfig {
  OutboxConfig(
    max_attempts: Int,
    dlq_subject: Option(String),
    security: SecurityEvidence,
  )
}

pub type OutboxEntry {
  OutboxEntry(
    id: String,
    subject: String,
    payload: String,
    ttl_milliseconds: Option(Int),
    attempts: Int,
  )
}

pub type Outbox {
  Outbox(next_id: Int, config: OutboxConfig, entries: List(OutboxEntry))
}

pub type PublishCommand {
  PublishCommand(subject: String, headers: List(Header), payload: String)
}

pub type QueueLifecycle {
  QueueLifecycle(
    is_open: Bool,
    destroy_subject: String,
    reconnect_every_milliseconds: Int,
    reconnect_for_milliseconds: Int,
  )
}

pub fn from_incoming(
  subject: String,
  reply: Option(String),
  headers: List(Header),
  payload: String,
) -> LinearMessage {
  LinearMessage(
    subject: subject,
    reply: reply,
    headers: headers,
    payload: payload,
    is_linear: header_value(headers, linear_event_header) == Some(linear_event_type),
    is_destroyed: False,
  )
}

pub fn access(message: LinearMessage) -> #(Option(String), LinearMessage) {
  case message.is_destroyed {
    True -> #(None, message)
    False ->
      case message.is_linear {
        True -> #(Some(message.payload), destroy(message))
        False -> #(Some(message.payload), message)
      }
  }
}

pub fn destroy(message: LinearMessage) -> LinearMessage {
  LinearMessage(..message, payload: "", is_destroyed: True)
}

pub fn expire_unread(message: LinearMessage) -> LinearMessage {
  case message.is_linear {
    True -> destroy(message)
    False -> message
  }
}

pub fn publish_linear(
  subject: String,
  payload: String,
  ttl_milliseconds: Option(Int),
  security: SecurityEvidence,
) -> PublishCommand {
  let headers = linear_headers("direct", ttl_milliseconds, security)
  PublishCommand(subject: subject, headers: headers, payload: payload)
}

pub fn new_outbox(
  max_attempts: Int,
  dlq_subject: Option(String),
  security: SecurityEvidence,
) -> Outbox {
  let attempts = case max_attempts < 1 {
    True -> 1
    False -> max_attempts
  }
  Outbox(
    next_id: 0,
    config: OutboxConfig(
      max_attempts: attempts,
      dlq_subject: dlq_subject,
      security: security,
    ),
    entries: [],
  )
}

pub fn enqueue_linear(
  outbox: Outbox,
  subject: String,
  payload: String,
  ttl_milliseconds: Option(Int),
) -> #(Outbox, String) {
  let id = int.to_string(outbox.next_id + 1)
  let entry = OutboxEntry(
    id: id,
    subject: subject,
    payload: payload,
    ttl_milliseconds: ttl_milliseconds,
    attempts: 0,
  )
  #(
    Outbox(..outbox, next_id: outbox.next_id + 1, entries: list.append(outbox.entries, [entry])),
    id,
  )
}

pub fn build_outbox_publish(entry: OutboxEntry, security: SecurityEvidence) -> PublishCommand {
  PublishCommand(
    subject: entry.subject,
    headers: linear_headers(entry.id, entry.ttl_milliseconds, security),
    payload: entry.payload,
  )
}

pub fn acknowledge_published(outbox: Outbox, id: String) -> Outbox {
  Outbox(..outbox, entries: remove_entry(outbox.entries, id))
}

pub fn fail_publish(outbox: Outbox, id: String, reason: String) -> #(Outbox, Option(PublishCommand)) {
  case pop_entry(outbox.entries, id) {
    None -> #(outbox, None)
    Some(#(entry, remaining)) -> {
      let attempts = entry.attempts + 1
      let updated = OutboxEntry(..entry, attempts: attempts)
      case attempts >= outbox.config.max_attempts, outbox.config.dlq_subject {
        True, Some(dlq_subject) -> {
          let command = PublishCommand(
            subject: dlq_subject,
            headers: dlq_headers(updated, reason),
            payload: updated.payload,
          )
          #(Outbox(..outbox, entries: remaining), Some(command))
        }
        _, _ -> #(Outbox(..outbox, entries: list.append(remaining, [updated])), None)
      }
    }
  }
}

pub fn open_managed_queue(
  destroy_subject: String,
  reconnect_for_milliseconds: Int,
  reconnect_every_milliseconds: Int,
) -> QueueLifecycle {
  let every = case reconnect_every_milliseconds <= 0 {
    True -> 1000
    False -> reconnect_every_milliseconds
  }
  QueueLifecycle(
    is_open: True,
    destroy_subject: destroy_subject,
    reconnect_every_milliseconds: every,
    reconnect_for_milliseconds: reconnect_for_milliseconds,
  )
}

pub fn on_destroy_event(queue: QueueLifecycle, subject: String) -> QueueLifecycle {
  case queue.is_open && subject == queue.destroy_subject {
    True -> QueueLifecycle(..queue, is_open: False)
    False -> queue
  }
}

pub fn should_reconnect(queue: QueueLifecycle, elapsed_milliseconds: Int) -> Bool {
  queue.is_open && elapsed_milliseconds <= queue.reconnect_for_milliseconds
}

pub fn header_value(headers: List(Header), key: String) -> Option(String) {
  case headers {
    [] -> None
    [#(candidate, value), ..rest] ->
      case candidate == key {
        True -> Some(value)
        False -> header_value(rest, key)
      }
  }
}

fn linear_headers(id: String, ttl_milliseconds: Option(Int), security: SecurityEvidence) -> List(Header) {
  let base = [#(linear_event_header, linear_event_type), #(linear_outbox_id_header, id)]
  let with_ttl = case ttl_milliseconds {
    Some(ttl) -> list.append(base, [#(linear_ttl_header, int.to_string(ttl))])
    None -> base
  }
  apply_security(with_ttl, security)
}

fn dlq_headers(entry: OutboxEntry, reason: String) -> List(Header) {
  [
    #(linear_outbox_id_header, entry.id),
    #(linear_dlq_original_subject_header, entry.subject),
    #(linear_dlq_reason_header, reason),
  ]
}

fn apply_security(headers: List(Header), security: SecurityEvidence) -> List(Header) {
  let with_pqc = case security.pqc_public_key {
    Some(key) ->
      list.append(headers, [
        #(linear_pqc_algorithm_header, linear_pqc_algorithm),
        #(linear_pqc_public_key_header, key),
      ])
    None -> headers
  }
  case security.dpop_proof {
    Some(proof) -> list.append(with_pqc, [#(dpop_header, proof)])
    None -> with_pqc
  }
}

fn remove_entry(entries: List(OutboxEntry), id: String) -> List(OutboxEntry) {
  case entries {
    [] -> []
    [entry, ..rest] ->
      case entry.id == id {
        True -> rest
        False -> [entry, ..remove_entry(rest, id)]
      }
  }
}

fn pop_entry(entries: List(OutboxEntry), id: String) -> Option(#(OutboxEntry, List(OutboxEntry))) {
  case entries {
    [] -> None
    [entry, ..rest] ->
      case entry.id == id {
        True -> Some(#(entry, rest))
        False ->
          case pop_entry(rest, id) {
            None -> None
            Some(#(found, remaining)) -> Some(#(found, [entry, ..remaining]))
          }
      }
  }
}
