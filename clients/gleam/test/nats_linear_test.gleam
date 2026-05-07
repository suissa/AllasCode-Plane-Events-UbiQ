import gleeunit
import gleeunit/should
import gleam/list
import nats_linear

pub fn main() {
  gleeunit.main()
}

fn no_security() {
  nats_linear.SecurityEvidence(pqc_public_key: None, dpop_proof: None)
}

pub fn linear_access_is_single_use_test() {
  let message = nats_linear.from_incoming(
    "linear.subject",
    None,
    [#(nats_linear.linear_event_header, nats_linear.linear_event_type)],
    "secret",
  )

  let #(first, after_first) = nats_linear.access(message)
  let #(second, _) = nats_linear.access(after_first)

  first |> should.equal(Some("secret"))
  second |> should.equal(None)
}

pub fn ttl_destroys_unread_linear_payload_test() {
  let message = nats_linear.from_incoming(
    "linear.subject",
    None,
    [#(nats_linear.linear_event_header, nats_linear.linear_event_type)],
    "expires",
  )

  let expired = nats_linear.expire_unread(message)
  let #(payload, _) = nats_linear.access(expired)

  payload |> should.equal(None)
}

pub fn non_linear_payload_is_reusable_test() {
  let message = nats_linear.from_incoming("regular.subject", None, [], "reusable")
  let #(first, after_first) = nats_linear.access(message)
  let #(second, _) = nats_linear.access(after_first)

  first |> should.equal(Some("reusable"))
  second |> should.equal(Some("reusable"))
}

pub fn publish_builds_linear_headers_test() {
  let command = nats_linear.publish_linear("linear.subject", "payload", Some(25), no_security())

  nats_linear.header_value(command.headers, nats_linear.linear_event_header)
  |> should.equal(Some(nats_linear.linear_event_type))
  nats_linear.header_value(command.headers, nats_linear.linear_ttl_header)
  |> should.equal(Some("25"))
}

pub fn outbox_success_removes_entry_test() {
  let #(outbox, id) = nats_linear.new_outbox(3, None, no_security())
  |> nats_linear.enqueue_linear("linear.out", "payload", Some(25))

  let emptied = nats_linear.acknowledge_published(outbox, id)

  emptied.entries |> should.equal([])
}

pub fn outbox_retry_keeps_entry_test() {
  let #(outbox, id) = nats_linear.new_outbox(3, None, no_security())
  |> nats_linear.enqueue_linear("linear.out", "payload", None)

  let #(retrying, dlq) = nats_linear.fail_publish(outbox, id, "publish failed")

  dlq |> should.equal(None)
  list.length(retrying.entries) |> should.equal(1)
}

pub fn outbox_dlq_after_limit_test() {
  let #(outbox, id) = nats_linear.new_outbox(1, Some("linear.dlq"), no_security())
  |> nats_linear.enqueue_linear("linear.out", "payload", None)

  let #(after_failure, dlq) = nats_linear.fail_publish(outbox, id, "publish failed")

  after_failure.entries |> should.equal([])
  case dlq {
    Some(command) -> {
      command.subject |> should.equal("linear.dlq")
      nats_linear.header_value(command.headers, nats_linear.linear_dlq_original_subject_header)
      |> should.equal(Some("linear.out"))
      nats_linear.header_value(command.headers, nats_linear.linear_dlq_reason_header)
      |> should.equal(Some("publish failed"))
    }
    None -> panic as "expected DLQ command"
  }
}

pub fn security_headers_are_attached_test() {
  let security = nats_linear.SecurityEvidence(
    pqc_public_key: Some("kyber-public-key"),
    dpop_proof: Some("proof.jwt"),
  )
  let command = nats_linear.publish_linear("linear.secure", "payload", None, security)

  nats_linear.header_value(command.headers, nats_linear.linear_pqc_algorithm_header)
  |> should.equal(Some(nats_linear.linear_pqc_algorithm))
  nats_linear.header_value(command.headers, nats_linear.linear_pqc_public_key_header)
  |> should.equal(Some("kyber-public-key"))
  nats_linear.header_value(command.headers, nats_linear.dpop_header)
  |> should.equal(Some("proof.jwt"))
}

pub fn managed_queue_closes_only_on_destroy_event_test() {
  let queue = nats_linear.open_managed_queue("linear.destroy", 5000, 1000)

  let still_open = nats_linear.on_destroy_event(queue, "linear.other")
  let closed = nats_linear.on_destroy_event(still_open, "linear.destroy")

  still_open.is_open |> should.equal(True)
  closed.is_open |> should.equal(False)
}

pub fn managed_queue_reconnect_policy_test() {
  let queue = nats_linear.open_managed_queue("linear.destroy", 5000, 0)

  queue.reconnect_every_milliseconds |> should.equal(1000)
  nats_linear.should_reconnect(queue, 4000) |> should.equal(True)
  nats_linear.should_reconnect(queue, 6000) |> should.equal(False)
}
