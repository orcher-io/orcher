-- V075: drop the event gates table.
--
-- Events are delivered through the journal: an EventReceived entry in the run,
-- and the run handed out again to read it. The event_gates table was meant to
-- record a run waiting for an event, but nothing ever wrote a row to it; every
-- event delivery read it, found nothing, and went on. It is dropped with its
-- status type.
DROP TABLE IF EXISTS event_gates;
DROP TYPE IF EXISTS event_gate_status;
