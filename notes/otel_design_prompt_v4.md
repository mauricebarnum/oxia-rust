# Prompt: OpenTelemetry Instrumentation Design Document for a Rust Library (v4)

Use this prompt with a coding or research agent when the goal is to produce a **design document only**, not implementation code.

---

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
> - **Before beginning any analysis, verify that all four inputs below are fully resolved (not placeholder values). If any input is missing or still a placeholder, halt immediately and request the missing information. Do not proceed with partial inputs and do not infer or hallucinate repository structure.**
>
> **Inputs:**
> - Rust library to analyze: `<EXACT PATH OR GITHUB URL WITH COMMIT SHA>`
> - Existing Go library that serves as the behavioral reference: `<EXACT PATH OR GITHUB URL WITH TAG OR COMMIT SHA>`
> - Target OpenTelemetry Semantic Conventions Version: `<EXACT VERSION, e.g., v1.28.0>`
> - Additional repository docs/tests/examples to inspect: `<EXPLICIT LIST OF PATHS — do not leave open-ended>`
>
> **Objective:**
> - Analyze the Rust library and the Go reference implementation.
> - Produce a detailed design document for adding instrumentation using an idiomatic Rust telemetry layer aligned with OpenTelemetry standards.
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
>    - existing telemetry hooks / log emissions
>    - concurrency boundaries
>    - task spawning boundaries
>    - detached futures
>    - cancellation semantics
>
> 2. Produce a separate inventory of all Go instrumentation points (including traces, metrics, and structured log events).
>
> 3. Only after both inventories are complete may recommendations begin.
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
> - Which kinds of telemetry will be added (traces, metrics, logs).
> - What is out of scope.
>
> ### 2. Repository Findings
> - Summary of the Rust library's public API surface and internal architecture, based on direct inspection.
> - Summary of the Go library's instrumentation approach (traces, metrics, logs), based on direct inspection.
> - Explicit mapping between the Go instrumentation model and the Rust API surface.
> - Flag any Rust operations with no Go equivalent.
> - Flag any Go instrumentation with no Rust equivalent.
> - Include source references for all factual claims.
>
> ### 3. Goals, Constraints, and Assumptions
> - Goals for traces, metrics, error recording, log correlation, and context propagation.
> - Clear non-goals and deferred work.
> - **Telemetry Framework Choice:** Evaluate whether the library should use direct `opentelemetry` API crate instrumentation or rely on the idiomatic Rust `tracing` crate ecosystem (leveraging `tracing-opentelemetry` compatibility downstream). Explicitly justify this choice based on the library's existing logging/tracing patterns.
> - The library must only use the chosen framework's *API/facade layer* in-library. It must not configure exporters, providers, SDK pipelines, or application-level runtime setups internally.
> - State assumptions about no-op behavior when no SDK is installed by the consumer application.
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
> ### 5. Tracing and Logging Design
> - Tracer/Telemetry scope naming and instrumentation boundaries.
> - Span boundaries and naming conventions (use quoted strings for every proposed span name).
> - Parent/child behavior.
> - Attributes (use dotted notation for every attribute name, e.g., `db.system`).
> - Error handling, panic capture, and span status rules.
> - Performance-related recording considerations.
> - Sampling implications:
>   - head sampling visibility,
>   - expensive attribute computation,
>   - observability under partial sampling.
> - Logging and Event strategy:
>   - Identify how structured logs or events emitted by the reference library map to Rust log events, `tracing` events, or OTel log records.
>   - Ensure trace_id/span_id context propagation and correlation strategy.
>   - Interaction with downstream application logging subscribers.
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
> - Instrument reuse and initialization strategy.
> - **Semantic Convention Target:** For every proposed metric, span, or log attribute, align strictly with the target OpenTelemetry Semantic Convention Version specified in the inputs. Explicitly justify any non-standard attribute names.
>
> ### 7. Context Propagation and Concurrency Design
> - Whether explicit context should be accepted or passed via the public API.
> - Where context enters and exits the public API.
> - Async/concurrency implications.
> - Whether context propagation survives spawned task boundaries.
> - Background worker ownership implications.
> - Cancellation semantics and telemetry implications.
> - **`tracing` Crate Interaction Details:** If the library currently uses the `tracing` crate, or if `tracing` is selected as the instrumentation facade in Section 3, recommend exactly one of the following and justify the choice:
>   - Bridge via `tracing-opentelemetry` (maintain `tracing` spans, propagate to OTel).
>   - Replace `tracing` spans entirely with direct OTel API spans.
>   - Maintain both independently.
>   - If the library does not use and will not use `tracing`, state this explicitly.
>
> ### 8. Public API and Internal Structure Impact
>
> This section must make a concrete recommendation on feature-gating. Recommend exactly one of the following options and state why the other two were rejected:
>
> - **Option A:** Always compiled, API-only. Relies on the underlying telemetry API crate's no-op behavior when no SDK is active.
> - **Option B:** Feature-gated, opt-in. Instrumentation compiled only when downstream crates enable a named feature flag (e.g., `telemetry` or `opentelemetry`).
> - **Option C:** Feature-gated, opt-out. Instrumentation enabled by default but can be disabled via disabling default features.
>
> Also include:
> - Any proposed public API changes.
> - Any internal module boundaries or helper modules.
> - MSRV (Minimum Supported Rust Version) impact.
> - Dependency tree impact.
> - Optional dependency strategy.
> - Compile-time cost implications.
> - Preference for minimizing unrelated refactoring.
>
> ### 9. Semantic Conventions and Cross-Language Consistency
> - Which semantic conventions apply based on the target version.
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
> - How spans, logs, and metrics should be verified in test suites.
> - Success and failure path expectations.
> - Expected behavior when no SDK/provider is active.
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
> - "Utilize the `tracing` crate facade with a target dependency on `tracing-opentelemetry` compatibility; do not take a direct dependency on `opentelemetry-sdk`."
>
> ### B.2 Assumptions
> - Repository assumptions (structure, Cargo features, MSRV, async runtime).
> - API assumptions (OTel/Tracing API versions, target semantic convention version).
> - Runtime assumptions (no-op behavior, SDK installation responsibility).
>
> ### B.3 Per-Operation Instrumentation Spec
>
> To avoid formatting degradation and generation drift, high-level structural properties are mapped in the summary table below, followed by detailed markdown specification blocks for each operation.
>
> #### Summary Mapping Table
>
> | Rust API Entry Point | Span / Event Name | Type (Span Kind / Log Event) | Metrics Emitted | Performance Classification |
> |---|---|---|---|---|
> | *e.g., `Client::connect`* | `"client.connect"` | `SpanKind::Client` | `"client.connect.duration"` | Warm path |
>
> #### Detailed Operation Specifications
>
> Provide a structured markdown block for **every** operation listed in the summary table above using the following format:
>
> ---
> ##### Operation: `[INSERT RUST API ENTRY POINT]`
> * **Purpose:** [Brief description of the operation]
> * **Span Name:** `"[SPAN NAME]"` (or `—` if event-only)
> * **Span Kind:** `[e.g., Client / Server / Internal / —]`
> * **Parent Context Behavior:** [e.g., Attaches to implicit parent context, creates detached root context, etc.]
> * **Attributes:**
>   * `[attribute.name.one]`: [Source value / type / semantic convention reference]
>   * `[attribute.name.two]`: [Source value / type / semantic convention reference]
> * **Error / Status Behavior:** [Exact conditions for setting status to `Error`, descriptive error strings, panic handling details]
> * **Performance Notes:** [Allocation behavior, attribute construction overhead, mitigation tactics]
> * **Notes & Tradeoffs:** [Any unique context or idiomatic variations from the Go reference]
> ---
>
> ### B.4 Metric Inventory
>
> | Metric Name | Instrument Type | Unit | Attributes | Attribute Cardinality | Semantic Convention Version | Lifecycle (init location) | Cardinality Warnings |
> |---|---|---|---|---|---|---|---|
>
> ### B.5 Module and File Plan
>
> List