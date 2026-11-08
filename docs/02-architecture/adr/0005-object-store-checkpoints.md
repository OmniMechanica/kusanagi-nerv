# ADR-0005: Durable object storage for training checkpoints and model artifacts

**Status:** Accepted for capstone. **Date:** 2026-11-08.

## Context

Worker-local disks are ephemeral. Retrying from epoch zero does not demonstrate reliable resume of expensive training.

## Decision

Store versioned checkpoint/artifact objects in MinIO/S3, include model, optimizer and epoch/step metadata, and prove restore via an injected worker failure.

## Alternatives

Local volume (insufficient across nodes), database BLOB storage (not optimal for large artifacts), external registry (possible future integration).

## Consequences

Object store availability, partial writes, serialization safety, object integrity, versioning and artifact permissions become explicit engineering concerns.
