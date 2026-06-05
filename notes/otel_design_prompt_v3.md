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
> - Do not invent repository structure, modules, helper utilities, extension points, or existing telemetry infrastructure.
> - If a capability does not exist in the repository, state that explicitly rather than assuming infrastructure exists.
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
> ## Repository Analysis Requirements
>
> Before proposing any design:
>
> 1. Produce a complete inventory of:
>    - public APIs
>    - async entry points
>    - background tasks
>    - retry loops
>    - network operations
>    - streaming operations
>    - batching operations
>    - error types
>    - existing telemetry hooks
>    - concurrency boundaries
>    - task spawning boundaries
>    - detached futures
>    - cancellation semantics
>
> 2. Produce a separate inventory of all Go instrumentation points.
>
> 3. Only after both inventories are complete may recommendations begin.
>
> 4. Every factual statement about the repositories must include direct inspection evidence:
>    - file path
>    - symbol/function/type name
>    - and line numbers where practical
>
> 5. Clearly distinguish statement types:
>    - Prefix repository-derived statements with `Observed:`
>    - Prefix recommendations with `Proposed:`
>    - Prefix assumptions with `Assumption:`
>
> ---
>
> ## Analysis Workflow
>
> The final document must clearly separate:
>
> 1. Repository inspection
> 2. Behavioral extraction from the Go implementation
> 3. Mapping analysis
> 4. Instrumentation design recommendations
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
> - Flag any Rust operations with no Go equivalent.
> - Flag any Go instrumentation with no Rust equivalent.
> - Include source references for all factual claims.
>
> ### 3. Goals, Constraints, and Assumptions
> - Goals for traces, metrics, error recording, and context propagation.
> - Clear non-goals and deferred work.
> - The Rust library uses the OpenTelemetry API crate in-library (`opentelemetry` crate, not `opentelemetry-sdk`).
> - The library must not configure exporters, providers, SDK pipelines, or application-level runtime setup internally.
> - State assumptions about no-op behavior when no SDK is installed by the application.
> - Analyze residual callsite and branching overhead even when no SDK is installed.
>
> ### 4. Instrumentation Boundaries
> - Which public or semantically meaningful operations should be instrumented.
> - Which operations should not be instrumented and why.
> - How the design avoids duplicate instrumentation from lower layers.
> - For every major architectural decision:
>   - list the selected approach,
>   - list at least one rejected alternative,
>   - explain why the alternative was rejected.
>
> ### 5. Tracing Design
> - Tracer naming and instrumentation scope.
> - Span boundaries.
> - Span naming conventions (use quoted strings for every proposed span name).
> - Parent/child behavior.
> - Attributes (use dotted notation for every attribute name, e.g., `db.system`).
> - Error handling and status rules.
> - Performance-related recording considerations.
> - Sampling implications:
>   - head sampling visibility,
>   - expensive attribute computation,
>   - observability under partial sampling.
> - Logging correlation strategy:
>   - trace_id/span_id propagation,
>   - interaction with tracing subscribers,
>   - whether logs should be enriched automatically or left to applications.
>
> ### 6. Metrics Design
> - Metrics to emit.
> - Instrument type for each metric and rationale.
> - Metric names (use quoted strings), units, dimensions/attributes, and cardinality considerations.
> - State assumptions about:
>   - synchronous vs asynchronous instruments,
>   - monotonicity,
>   - temporality expectations,
>   - aggregation compatibility.
> - **Cardinality constraint:** For every metric attribute, classify it explicitly as either:
>   - *Bounded* — document the bound and its source.
>   - *Potentially unbounded* — flag it as a risk requiring explicit reviewer sign-off before implementation proceeds.
>   - No attribute with user-controlled or operationally unbounded cardinality may be included without a risk flag.
> - Instrument reuse strategy.
> - For every proposed metric attribute and span attribute:
>   - identify the semantic convention source if one exists,
>   - specify the exact convention version,
>   - explicitly justify any non-standard attribute names.
>
> ### 7. Context Propagation and Concurrency Design
>
> This section is required regardless of whether the library currently uses the `tracing` crate.
>
> - Whether explicit context should be accepted or passed via the public API.
> - Where context enters and exits the public API.
> - Async/concurrency implications.
> - Whether context propagation survives spawned task boundaries.
> - Background worker ownership implications.
> - Cancellation semantics and telemetry implications.
> - **`tracing` crate interaction (required):** State whether the library currently uses the `tracing` crate. If it does, the document must recommend exactly one of the following and justify the choice:
>   - Bridge via `opentelemetry-tracing` (maintain `tracing` spans, propagate to OTel).
>   - Replace `tracing` spans with direct OTel spans.
>   - Maintain both independently.
>   - If the library does not use `tracing`, state this explicitly.
>
> ### 8. Public API and Internal Structure Impact
>
> This section must make a concrete recommendation on feature-gating. Recommend exactly one of the following options and state why the other two were rejected:
>
> - **Option A:** Always compiled, API-only. Relies on the OpenTelemetry API crate's no-op provider behavior when no SDK is installed.
> - **Option B:** Feature-gated, opt-in. Instrumentation compiled only when the downstream crate enables a named feature (e.g., `opentelemetry`).
> - **Option C:** Feature-gated, opt-out. Instrumentation enabled by default but can be disabled.
>
> Also include:
> - Any proposed public API changes.
> - Any internal module boundaries or helper modules.
> - MSRV impact.
> - Dependency tree impact.
> - Optional dependency strategy.
> - Compile-time cost implications.
> - Preference for minimizing unrelated refactoring.
>
> ### 9. Semantic Conventions and Cross-Language Consistency
> - Which semantic conventions apply.
> - Which naming/attribute choices are preserved from the Go implementation.
> - Which design choices intentionally diverge for idiomatic Rust reasons, and why.
> - Telemetry API stability analysis:
>   - span name stability,
>   - metric name stability,
>   - attribute schema evolution,
>   - cardinality migration risks,
>   - downstream dashboard compatibility.
>
> ### 10. Performance and Risk Analysis
> - Allocation risks.
> - Hot-path overhead.
> - Cardinality risks (cross-reference Section 6).
> - Mitigation strategies for each identified risk.
> - For every proposed instrumented operation classify it as:
>   - Hot path
>   - Warm path
>   - Cold path
> - For hot-path operations:
>   - justify allocation behavior,
>   - locking behavior,
>   - attribute construction cost.
>
> ### 11. Rollout and Migration Plan
> - Backward compatibility considerations.
> - Feature flag or rollout strategy (consistent with the decision made in Section 8).
> - Implications for downstream applications.
> - A phased implementation plan.
> - Recommended PR or commit breakdown.
> - Risks, dependencies, and rollback considerations.
>
> ### 12. Testing and Validation Strategy
> - What tests should later be added.
> - How spans and metrics should be verified.
> - Success and failure path expectations.
> - Expected behavior when no SDK/provider is installed.
>
> ### 13. Open Questions
> - Unresolved decisions requiring reviewer input before implementation begins.
> - Clearly distinguish:
>   - questions the reviewer must answer,
>   - questions the implementation agent can resolve independently.
>
> ---
>
> ## Part B: Implementation Handoff Spec
>
> This section is for a later implementation agent. It must be concrete, compact, and unambiguous. The implementation agent will have no context beyond this document and the repository. Do not imply behavior — state it. Call out tradeoffs explicitly where multiple designs are viable.
>
> ### B.1 Required Decisions
>
> List every decision already made by this design as implementation requirements, not suggestions. Format as a checklist.
>
> Example:
> - "Use `opentelemetry` API crate only; do not take a direct dependency on `opentelemetry-sdk`."
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
> | Rust API Entry Point | Purpose | Span Name | Span Kind | Parent Context Behavior | Attributes | Metrics Emitted | Error/Status Behavior | Performance Classification | Performance Notes | Notes/Tradeoffs |
> |---|---|---|---|---|---|---|---|---|---|---|
>
> ### B.4 Metric Inventory
>
> | Metric Name | Instrument Type | Unit | Attributes | Attribute Cardinality | Semantic Convention Source | Lifecycle (init location) | Cardinality Warnings |
> |---|---|---|---|---|---|---|---|
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
> A numbered, step-by-step checklist an implementation agent can execute sequentially. Steps must be concrete actions, not goals.
>
> Example:
> - "Add `opentelemetry = { version = \"X.Y\", optional = true }` to `Cargo.toml` under `[dependencies]`."
>
> ### B.7 Acceptance Criteria
>
> A checklist of verifiable completion conditions. Each criterion must be checkable without subjective judgment.
>
> **Required format:**
> - Prefer: *"Span `\"operation.name\"` is emitted with attribute `foo.bar` set to `\"value\"` when condition X occurs."*
> - Prefer: *"Metric `metric.name` increments by 1 on each call to `fn_name` when Y."*
> - Do not write: *"Spans are emitted as expected."* or *"Instrumentation is correct."*
>
> ### B.8 Optional Enhancements
>
> Nice-to-have follow-up work that must not block the first implementation. For each item, state why it is deferred rather than included.
>
> ### B.9 Reviewer Questions
>
> Questions that must be answered before implementation begins, if any remain after Part A Section 13.
>
> If none remain, state:
>
> `No blocking questions. Implementation may proceed.`
>
> ---
>
> ## Final Consistency Audit (Required)
>
> Before finalizing the document, perform a consistency audit:
>
> - Every span referenced in prose must appear in the operation table.
> - Every metric must appear in the metric inventory.
> - Every attribute must use consistent naming.
> - Every feature flag reference must be consistent.
> - Every proposed file modification must correspond to described functionality.
> - Every stated assumption must either be verified or explicitly labeled unverified.
> - Every repository claim must include direct inspection evidence.
> - Every semantic convention reference must specify a version.
> - Every potentially unbounded metric dimension must be explicitly flagged.
> - Every rejected alternative must include rationale.
>
> Do not finalize the document until all audit checks pass.

