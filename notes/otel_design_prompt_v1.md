# Prompt: OpenTelemetry Instrumentation Design Document for a Rust Library

Use this prompt with a coding or research agent when the goal is to produce a **design document only**, not implementation code.

## Prompt

> Produce a detailed Markdown design document for adding OpenTelemetry instrumentation to this Rust library.
>
> Important constraints:
> - Do **not** implement code.
> - Do **not** modify source files unless explicitly asked.
> - The primary deliverable is a design and handoff document for later implementation.
> - Assume the implementation may be done by a different agent than the one producing this design.
>
> Inputs:
> - Rust library to analyze: `<PATH TO RUST LIBRARY>`
> - Existing Go library that serves as the behavioral reference: `<PATH OR URL TO GO LIBRARY>`
> - Additional repository docs/tests/examples to inspect: `<PATHS>`
>
> Objective:
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
> The document must have two major parts:
>
> ## Part A: Design Narrative
>
> This section is for human review. It should explain the design clearly, justify important choices, and make tradeoffs legible.
>
> Include these sections:
>
> 1. **Executive summary**
>    - What the proposal does.
>    - Which kinds of telemetry will be added.
>    - What is out of scope.
>
> 2. **Repository findings**
>    - Summary of the Rust library’s public API surface and internal architecture.
>    - Summary of the Go library’s instrumentation approach.
>    - Mapping between the Go model and the Rust library.
>
> 3. **Goals and non-goals**
>    - Goals for traces, metrics, error recording, and context propagation.
>    - Clear non-goals and deferred work.
>
> 4. **OpenTelemetry constraints and assumptions**
>    - Explain that the Rust library should use the OpenTelemetry API in-library.
>    - Explain that the library should not configure exporters, providers, SDK pipelines, or application-level runtime setup internally.
>    - State assumptions about no-op behavior when no SDK is installed by the application.
>
> 5. **Instrumentation boundaries**
>    - Which public or semantically meaningful operations should be instrumented.
>    - Which operations should not be instrumented.
>    - How the design avoids duplicate instrumentation from lower layers.
>
> 6. **Tracing design**
>    - Tracer naming and instrumentation scope.
>    - Span boundaries.
>    - Span naming conventions.
>    - Parent/child behavior.
>    - Attributes.
>    - Error handling and status rules.
>    - Performance-related recording considerations.
>
> 7. **Metrics design**
>    - Metrics to emit.
>    - Instrument type for each metric and rationale.
>    - Metric names, units, dimensions/attributes, and cardinality considerations.
>    - Instrument reuse strategy.
>
> 8. **Context propagation design**
>    - Whether explicit context should be accepted or passed.
>    - Where context enters and exits the public API.
>    - Async/concurrency implications.
>    - Interaction with any existing `tracing` usage if relevant.
>
> 9. **Public API and internal structure impact**
>    - Any proposed public API changes.
>    - Any internal module boundaries or helper modules.
>    - Whether instrumentation should be feature-gated, configurable, or always compiled as API-only support.
>
> 10. **Semantic conventions and cross-language consistency**
>     - Which semantic conventions apply.
>     - Which naming/attribute choices are preserved from the Go implementation.
>     - Which design choices intentionally diverge for idiomatic Rust reasons.
>
> 11. **Performance and risk analysis**
>     - Allocation risks.
>     - Hot-path overhead.
>     - Cardinality risks.
>     - Any mitigation strategies.
>
> 12. **Compatibility and migration**
>     - Backward compatibility considerations.
>     - Feature flags or rollout strategy.
>     - Implications for downstream applications.
>
> 13. **Testing and validation strategy**
>     - What tests should later be added.
>     - How spans and metrics should be verified.
>     - Success and failure path expectations.
>     - Expectations when no SDK/provider is installed.
>
> 14. **Implementation sequencing**
>     - A phased implementation plan.
>     - Recommended PR or commit breakdown.
>     - Risks, dependencies, and rollback considerations.
>
> 15. **Open questions**
>     - Unresolved decisions.
>     - Ambiguities requiring reviewer input.
>
> ## Part B: Implementation Handoff Spec
>
> This section is for a later implementation agent. It must be concrete, compact, and unambiguous.
>
> Include these sections:
>
> 1. **Required decisions**
>    - Decisions already made by the design.
>    - These should be written as implementation requirements, not suggestions.
>
> 2. **Assumptions**
>    - Repository assumptions.
>    - API assumptions.
>    - Runtime assumptions.
>
> 3. **Per-operation instrumentation spec**
>    - Provide a compact table with one row per instrumented operation.
>    - Include these columns where applicable:
>      - Rust API entry point
>      - Purpose of operation
>      - Proposed span name
>      - Span kind if relevant
>      - Parent context behavior
>      - Attributes
>      - Metrics emitted
>      - Error/status behavior
>      - Performance notes
>      - Notes/tradeoffs
>
> 4. **Metric inventory**
>    - A table listing each metric, instrument type, unit, attributes, lifecycle, and cardinality warnings.
>
> 5. **Module and file plan**
>    - Proposed files/modules to create or update.
>    - What responsibility lives in each place.
>
> 6. **Implementation checklist**
>    - A step-by-step checklist that another agent could execute.
>
> 7. **Acceptance criteria**
>    - A precise checklist that determines whether implementation is complete.
>
> 8. **Optional enhancements**
>    - Nice-to-have follow-up work that should not block the first implementation.
>
> 9. **Reviewer questions**
>    - Questions that should be answered before implementation begins, if any remain.
>
> Multi-agent handoff requirements:
> - Optimize for handoff quality over brevity.
> - Assume the implementation agent will have no hidden context beyond this document and the repository.
> - Distinguish clearly between required decisions, assumptions, optional enhancements, and unresolved questions.
> - Do not leave important behavior implied.
> - Call out tradeoffs explicitly when multiple designs are viable.
> - Prefer exact names, exact boundaries, and exact acceptance criteria over general guidance.
>
> Analysis workflow:
> - Inspect the Go code first and extract the instrumentation model: operations, spans, attributes, metrics, error handling, and context behavior.
> - Then inspect the Rust library and map the Go model onto the Rust public API.
> - Recommend an idiomatic Rust design using the `opentelemetry` crate API.
> - Follow OpenTelemetry guidance for libraries: instrument meaningful library-level operations, prefer semantic conventions where applicable, and avoid embedding SDK/export configuration in the library.
> - Make all assumptions explicit.
> - Where there are multiple valid approaches, compare them and recommend one.
>
> Output constraints:
> - The final output must be the Markdown design document only.
> - Do not generate implementation code, except for tiny pseudocode snippets if needed to clarify structure.
> - End the document with a short section titled `Implementation Handoff Notes` aimed at a separate implementation agent.
