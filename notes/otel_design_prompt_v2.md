# Prompt: OpenTelemetry Instrumentation Design Document for a Rust Library

Use this prompt with a coding or research agent when the goal is to produce a **design document only**, not implementation code.

## Prompt

> Produce a detailed Markdown design document for adding OpenTelemetry instrumentation to this Rust library.
>
> **Important constraints:**
> - Do **not** implement code.
> - Do **not** modify source files unless explicitly asked.
> - The primary deliverable is a design and handoff document for later implementation.
> - Assume the implementation may be done by a different agent than the one producing this design.
> - **Before beginning any analysis, verify that all three inputs below are fully resolved (not placeholder values). If any input is missing or still a placeholder, halt immediately and request the missing information. Do not proceed with partial inputs and do not infer or hallucinate repository structure.**
>
> **Inputs:**
> - Rust library to analyze: `<EXACT PATH OR GITHUB URL WITH COMMIT SHA>`
> - Existing Go library that serves as the behavioral reference: `<EXACT PATH OR GITHUB URL WITH TAG OR COMMIT SHA>`
> - Additional repository docs/tests/examples to inspect: `<EXPLICIT LIST OF PATHS — do not leave open-ended>`
>
> **Objective:**
> - Analyze the Rust library and the Go reference implementation.
> - Produce a detailed design document for adding instrumentation using the Rust `opentelemetry` crate API.
> - Use the Go library as a reference for scope, concepts, and telemetry behavior, but recommend an idiomatic Rust design rather than a literal translation.
>
> The document must be suitable for two audiences:
> 1. A human reviewer evaluating whether the design is correct.
> 2. A separate implementation agent that will later execute the design with minimal ambiguity.
>
> Write the output as a Markdown file named `otel-instrumentation-design.md`.
>
> ---
>
> ## Analysis Workflow
>
> Execute in this order. Do not reorder steps.
>
> 1. **Inspect the Rust library first.** Catalog: the full public API surface, key internal modules, error types, async boundaries, concurrency model, and any existing telemetry (including any use of the `tracing` crate).
> 2. **Inspect the Go library second.** Extract: every instrumented operation, span names and kinds, attributes, metrics, error handling patterns, context propagation approach, and any SDK/exporter configuration the Go library does or does not perform.
> 3. **Produce the mapping.** Map each Go instrumented operation to its Rust API equivalent. For Rust operations with no direct Go analog, flag them explicitly and recommend instrumentation independently.
> 4. **Recommend idiomatic Rust instrumentation** using the `opentelemetry` crate API. Follow OpenTelemetry library instrumentation guidance: instrument meaningful library-level operations, prefer semantic conventions, and do not embed SDK or exporter configuration in the library.
> 5. **Make all assumptions explicit.** Where multiple valid approaches exist, compare them and recommend one with a stated rationale. Do not leave viable alternatives unmentioned.
>
> ---
>
> The document must have two major parts:
>
> ## Part A: Design Narrative
>
> This section is for human review. Explain the design clearly, justify important choices, and make tradeoffs legible.
>
> ### 1. Executive Summary
> - What the proposal does.
> - Which kinds of telemetry will be added.
> - What is out of scope.
>
> ### 2. Repository Findings
> - Summary of the Rust library's public API surface and internal architecture, based on direct inspection.
> - Summary of the Go library's instrumentation approach, based on direct inspection.
> - Explicit mapping between the Go instrumentation model and the Rust API surface.
> - Flag any Rust operations with no Go equivalent; flag any Go instrumentation with no Rust equivalent.
>
> ### 3. Goals and Non-Goals
> - Goals for traces, metrics, error recording, and context propagation.
> - Clear non-goals and deferred work.
>
> ### 4. OpenTelemetry Constraints and Assumptions
> - The Rust library uses the OpenTelemetry API crate in-library (`opentelemetry` crate, not `opentelemetry-sdk`).
> - The library must not configure exporters, providers, SDK pipelines, or application-level runtime setup internally.
> - State assumptions about no-op behavior when no SDK is installed by the application.
>
> ### 5. Instrumentation Boundaries
> - Which public or semantically meaningful operations should be instrumented.
> - Which operations should not be instrumented and why.
> - How the design avoids duplicate instrumentation from lower layers.
>
> ### 6. Tracing Design
> - Tracer naming and instrumentation scope.
> - Span boundaries.
> - Span naming conventions (use quoted strings for every proposed span name).
> - Parent/child behavior.
> - Attributes (use dotted notation for every attribute name, e.g., `db.system`).
> - Error handling and status rules.
> - Performance-related recording considerations.
>
> ### 7. Metrics Design
> - Metrics to emit.
> - Instrument type for each metric and rationale.
> - Metric names (use quoted strings), units, dimensions/attributes, and cardinality considerations.
> - **Cardinality constraint:** For every metric attribute, classify it explicitly as either:
>   - *Bounded* — document the bound and its source.
>   - *Potentially unbounded* — flag it as a risk requiring explicit reviewer sign-off before implementation proceeds.
>   - No attribute with user-controlled or operationally unbounded cardinality may be included without a risk flag.
> - Instrument reuse strategy.
>
> ### 8. Context Propagation Design
>
> This section is required regardless of whether the library currently uses the `tracing` crate.
>
> - Whether explicit context should be accepted or passed via the public API.
> - Where context enters and exits the public API.
> - Async/concurrency implications.
> - **`tracing` crate interaction (required):** State whether the library currently uses the `tracing` crate. If it does, the document must recommend exactly one of the following and justify the choice:
>   - Bridge via `opentelemetry-tracing` (maintain `tracing` spans, propagate to OTel).
>   - Replace `tracing` spans with direct OTel spans.
>   - Maintain both independently.
>   - If the library does not use `tracing`, state this explicitly.
>
> ### 9. Public API and Internal Structure Impact
>
> This section must make a concrete recommendation on feature-gating. Recommend exactly one of the following options and state why the other two were rejected:
>
> - **Option A:** Always compiled, API-only. No feature flag. Relies on OTel API's no-op behavior when no SDK is present. Zero overhead without SDK installed.
> - **Option B:** Feature-gated, opt-in. Instrumentation compiled only when the downstream crate enables a named feature (e.g., `opentelemetry`).
> - **Option C:** Feature-gated, opt-out. Instrumentation enabled by default but can be disabled.
>
> Also include:
> - Any proposed public API changes.
> - Any internal module boundaries or helper modules.
>
> ### 10. Semantic Conventions and Cross-Language Consistency
> - Which semantic conventions apply.
> - Which naming/attribute choices are preserved from the Go implementation.
> - Which design choices intentionally diverge for idiomatic Rust reasons, and why.
>
> ### 11. Performance and Risk Analysis
> - Allocation risks.
> - Hot-path overhead.
> - Cardinality risks (cross-reference Section 7).
> - Mitigation strategies for each identified risk.
>
> ### 12. Compatibility and Migration
> - Backward compatibility considerations.
> - Feature flag or rollout strategy (consistent with the decision made in Section 9).
> - Implications for downstream applications.
>
> ### 13. Testing and Validation Strategy
> - What tests should later be added.
> - How spans and metrics should be verified.
> - Success and failure path expectations.
> - Expected behavior when no SDK/provider is installed.
>
> ### 14. Implementation Sequencing
> - A phased implementation plan.
> - Recommended PR or commit breakdown.
> - Risks, dependencies, and rollback considerations.
>
> ### 15. Open Questions
> - Unresolved decisions requiring reviewer input before implementation begins.
> - Clearly distinguish: questions the reviewer must answer vs. questions the implementation agent can resolve independently.
>
> ---
>
> ## Part B: Implementation Handoff Spec
>
> This section is for a later implementation agent. It must be concrete, compact, and unambiguous. The implementation agent will have no context beyond this document and the repository. Do not imply behavior — state it. Call out tradeoffs explicitly where multiple designs are viable.
>
> ### B.1 Required Decisions
>
> List every decision already made by this design as implementation requirements, not suggestions. Format as a checklist. Example: "Use `opentelemetry` API crate only; do not take a direct dependency on `opentelemetry-sdk`."
>
> ### B.2 Assumptions
> - Repository assumptions (structure, Cargo features, MSRV, async runtime).
> - API assumptions (OTel API version, semantic convention version).
> - Runtime assumptions (no-op behavior, SDK installation responsibility).
>
> ### B.3 Per-Operation Instrumentation Spec
>
> Provide a table with one row per instrumented operation.
>
> **Table rules:**
> - Every instrumented operation gets its own row. Do not combine operations.
> - If a column value is not applicable, write `—` explicitly. Do not omit the cell.
> - Span names must be quoted strings (e.g., `"db.query"`), not prose descriptions.
> - Attribute names must use dotted notation (e.g., `db.system`), not prose.
>
> Columns:
>
> | Rust API Entry Point | Purpose | Span Name | Span Kind | Parent Context Behavior | Attributes | Metrics Emitted | Error/Status Behavior | Performance Notes | Notes/Tradeoffs |
> |---|---|---|---|---|---|---|---|---|---|
>
> ### B.4 Metric Inventory
>
> | Metric Name | Instrument Type | Unit | Attributes | Attribute Cardinality | Lifecycle (init location) | Cardinality Warnings |
> |---|---|---|---|---|---|---|
>
> ### B.5 Module and File Plan
>
> List every file to create or modify. For each:
> - File path relative to crate root.
> - Whether it is new or modified.
> - Its single-sentence responsibility.
> - Any feature flag gating it.
>
> ### B.6 Implementation Checklist
>
> A numbered, step-by-step checklist an implementation agent can execute sequentially. Steps must be concrete actions, not goals. Example: "Add `opentelemetry = { version = "X.Y", optional = true }` to `Cargo.toml` under `[dependencies]`."
>
> ### B.7 Acceptance Criteria
>
> A checklist of verifiable completion conditions. Each criterion must be checkable without subjective judgment.
>
> **Required format:**
> - Prefer: *"Span `"operation.name"` is emitted with attribute `foo.bar` set to `"value"` when condition X occurs."*
> - Prefer: *"Metric `metric.name` increments by 1 on each call to `fn_name` when Y."*
> - Do not write: *"Spans are emitted as expected."* or *"Instrumentation is correct."*
>
> ### B.8 Optional Enhancements
>
> Nice-to-have follow-up work that must not block the first implementation. For each item, state why it is deferred rather than included.
>
> ### B.9 Reviewer Questions
>
> Questions that must be answered before implementation begins, if any remain after Part A Section 15. If none remain, state: "No blocking questions. Implementation may proceed."
